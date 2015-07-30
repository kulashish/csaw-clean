package iitb.CSAW.Corpus.Webaroo;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectLongHashMap;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Specific to Webaroo source. Three configuration CSV files are needed:
 * <ul>
 * <li>DiskStripe,FilePrefix --- tells us where each out_data_ file resides.</li>
 * <li>DiskStripe,HostStripe[,HostStripe]* --- buddy group that accesses a disk
 * stripe. Usually the stripe is replicated over all buddies in the list.
 * One host stripe can be assigned at most one disk stripe. This may be an
 * undesirable limitation.</li>
 * <li>HostStripe,HostName --- maps between a logical host to a physical host name.</li>
 * </ul>
 * 
 * @author soumen
 */
public class WebarooStripeManager extends AStripeManager {
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final int nDiskStripes, nHostStripes;
	final int myHostStripe, myDiskStripe;
	final String myHostName;
	final int nBuddies, myBuddyIndex;
	
	/**
	 * Note that {@link FileColumns#NumDocs} is the number of documents in each
	 * {@link FileColumns#File}, i.e., it is not cumulative.  However, this file
	 * is read line by line in sequence, and that decides the document ID range 
	 * assigned to each {@link FileColumns#File}.  Note that this means that
	 * {@link FileColumns#DiskStripe} must be <b>nondecreasing</b>.
	 */
	enum FileColumns { DiskStripe, CumulativeIgnored, BlankIgnored, BytesIgnored, File, NumDocs, HostStripeIgnored, SIZE };
	
	/**
	 * Line format is <tt>DiskStripe(,HostStripe)+</tt>
	 * So a line has a variable number of tokens.
	 * But usually the number of buddies replicating a stripe is the same
	 * for all stripes.
	 */
	enum HostColumns { HostStripe, HostName, SIZE };
	
	private final TObjectIntHashMap<String> hostNameToStripe = new TObjectIntHashMap<String>();
	private final TIntObjectHashMap<String> hostStripeToName = new TIntObjectHashMap<String>();
	/** We will store only one way and invert during method calls because efficiency is not important. */
	private final TIntIntHashMap hostToDiskStripe = new TIntIntHashMap();

	final TObjectLongHashMap<String> prefixToNumDocs;
	final TObjectLongHashMap<String> prefixToBeginDocId;
	final ObjectArrayList<String> prefixArray;
	final IntArrayList cumulativeNumDocsArray;
	final TObjectIntHashMap<String> prefixToDiskStripe;

	/**
	 * @param config in+out, modified to add stripe information
	 * @throws IOException
	 * @throws ConfigurationException 
	 */
	public WebarooStripeManager(Config config) throws IOException, ConfigurationException {
		this.config = config;
		myHostName = InetAddress.getLocalHost().getHostName(); // not fqdn
		config.setProperty(hostNameKey, myHostName);
		
		final File stripeManagerDir = new File(config.getString(PropertyKeys.stripeManagerDirName));
		final File filesCsvFile = new File(stripeManagerDir, PropertyKeys.filesCsvName);
		final File hostsCsvFile = new File(stripeManagerDir, PropertyKeys.hostsCsvName);
		final File buddyCsvFile = new File(stripeManagerDir, PropertyKeys.buddyCsvName);
		
		final CSVReader hostsCsv = new CSVReader(new FileReader(hostsCsvFile), ',');
		hostsCsv.readNext(); // discard first line
		int myHostStripeLocal = -1;
		for (String line[] = null; (line = hostsCsv.readNext()) != null; ) {
			if (line.length == HostColumns.SIZE.ordinal()) {
				final String hostName = line[HostColumns.HostName.ordinal()];
				final int hostStripe = Integer.parseInt(line[HostColumns.HostStripe.ordinal()]);
				hostNameToStripe.put(hostName, hostStripe);
				hostStripeToName.put(hostStripe, hostName);
				if (hostName.equals(myHostName())) {
					myHostStripeLocal = Integer.parseInt(line[HostColumns.HostStripe.ordinal()]);
				}
			}
		}
		hostsCsv.close();
		if (myHostStripeLocal == -1) {
			throw new IllegalArgumentException("host " + myHostName() + " cannot find my stripe number");
		}
		this.myHostStripe = myHostStripeLocal;
		
		this.prefixArray = new ObjectArrayList<String>();
		this.cumulativeNumDocsArray = new IntArrayList();
		final CSVReader buddyCsv = new CSVReader(new FileReader(buddyCsvFile), ',');
		int myDiskStripeLocal = -1, nBuddiesLocal = -1, myBuddyIndexLocal = -1;
		int nDiskStripesLocal = -1, nHostStripesLocal = -1;
		for (String line[] = null; (line = buddyCsv.readNext()) != null; ) {
			if (!line[0].matches("\\d+")) {
				continue;
			}
			final int aDiskStripe = Integer.parseInt(line[0]);
			nDiskStripesLocal = Math.max(nDiskStripesLocal, aDiskStripe+1);
			assert line.length > 1;
			for (int buddyIndex = 0; buddyIndex < line.length - 1; ++buddyIndex) {
				final int hostStripeAtBuddyIndex = Integer.parseInt(line[1+buddyIndex]);
				nHostStripesLocal = Math.max(nHostStripesLocal, hostStripeAtBuddyIndex+1);
				hostToDiskStripe.put(hostStripeAtBuddyIndex, aDiskStripe);
				if (myHostStripe == hostStripeAtBuddyIndex) {
					if (myDiskStripeLocal != -1) {
						throw new IllegalArgumentException("Host stripe " + myHostStripe + " assigned to multiple disk stripes");
					}
					myDiskStripeLocal = Integer.parseInt(line[0]);
					myBuddyIndexLocal = buddyIndex;
					nBuddiesLocal = line.length - 1;
				}
			}
		}
		this.myDiskStripe = myDiskStripeLocal;
		this.nBuddies = nBuddiesLocal;
		this.myBuddyIndex = myBuddyIndexLocal;
		this.nDiskStripes = nDiskStripesLocal;
		this.nHostStripes = nHostStripesLocal;
		buddyCsv.close();
		
		prefixToNumDocs = new TObjectLongHashMap<String>();
		prefixToBeginDocId = new TObjectLongHashMap<String>();
		prefixToDiskStripe = new TObjectIntHashMap<String>();
		long beginDocId = 0;
		final CSVReader filesCsv = new CSVReader(new FileReader(filesCsvFile), ',');
		filesCsv.readNext(); // skip header line
		for (String line[] = null; (line = filesCsv.readNext()) != null; ) {
			if (line.length != FileColumns.SIZE.ordinal()) {
				continue;
			}
			final String fileName = line[FileColumns.File.ordinal()].trim();
			prefixToBeginDocId.put(fileName, beginDocId);
			if (line[FileColumns.NumDocs.ordinal()].trim().matches("\\d+")) {
				final long numDocs = Long.parseLong(line[FileColumns.NumDocs.ordinal()]); 
				beginDocId += numDocs;
				prefixToNumDocs.put(fileName, numDocs);
				prefixArray.add(fileName);
				if (beginDocId > Integer.MAX_VALUE) {
					throw new IllegalArgumentException("Cannot support " + beginDocId + " documents");
				}
				cumulativeNumDocsArray.add((int) beginDocId);
			}
			if (line[FileColumns.DiskStripe.ordinal()].trim().matches("\\d+")) {
				prefixToDiskStripe.put(fileName, Integer.parseInt(line[FileColumns.DiskStripe.ordinal()]));
			}
		}
//		for (String prefix : prefixToBeginDocId.keys(new String[]{})) {
//			System.out.println(prefix + " my=" + myStripeNumber + " disk=" + prefixToDiskStripe.get(prefix) + " begin=" + prefixToBeginDocId.get(prefix) + " num=" + prefixToNumDocs.get(prefix));
//		}
	}
	
	String sanitize(File dirOrFile) {
		final String name = dirOrFile.getName();
		Pattern pattern = Pattern.compile("^(out_data_\\d+).*?$");
		Matcher matcher = pattern.matcher(name);
		if (!matcher.matches()) {
			return null;
		}
		else {
			return matcher.group(1);
		}
	}
	
	@Deprecated
	public boolean isMyJob(File dirOrFile) {
		final String base = sanitize(dirOrFile);
		if (base == null) return false;
		if (diskStripe(dirOrFile) != myDiskStripe) return false;
		return Math.abs(base.hashCode()) % nBuddies == myBuddyIndex;
	}
	
	/**
	 * @param docId
	 * @return the corpus path prefix that contains this document, null if docId is beyond range
	 */
	@Deprecated
	public String docIdToPrefix(int docId) {
		assert cumulativeNumDocsArray.size() == prefixArray.size();
		for (int ds = 0; ds < cumulativeNumDocsArray.size(); ++ds) {
			if (docId < cumulativeNumDocsArray.getInt(ds)) {
				return prefixArray.get(ds);
			}
		}
		return null;
	}

	@Deprecated
	public int diskStripe(File dirOrFile) {
		final String base = sanitize(dirOrFile);
		if (base == null || !prefixToDiskStripe.containsKey(base)) {
			throw new IllegalArgumentException("Cannot find disk stripe for " + dirOrFile);
		}
		return prefixToDiskStripe.get(base);
	}
	
	long beginDocId(File dirOrFile) {
		final String base = sanitize(dirOrFile);
		if (base == null || !prefixToBeginDocId.containsKey(base)) {
			throw new IllegalArgumentException("Cannot find begin doc ID for " + dirOrFile);
		}
		return prefixToBeginDocId.get(base);
	}
	
	long numDocs(File dirOrFile) {
		final String base = sanitize(dirOrFile);
		if (base == null || !prefixToNumDocs.containsKey(base)) {
			throw new IllegalArgumentException("Cannot find numDocs in " + dirOrFile);
		}
		return prefixToNumDocs.get(base);
	}
	
	/* Compliance with standard stripe manager interface --- still quite incomplete. */

	@Override
	public String myHostName() {
		return myHostName;
	}

	@Override
	public int numDiskStripes() {
		return nDiskStripes;
	}

	@Override
	public int numHostStripes() {
		return nHostStripes;
	}

	@Override
	public IntList buddyHostStripes(final int aDiskStripe) {
		final IntArrayList ans = new IntArrayList();
		hostToDiskStripe.forEachEntry(new TIntIntProcedure() {
			@Override
			public boolean execute(int hostStripe, int diskStripe) {
				if (diskStripe == aDiskStripe && !ans.contains(hostStripe)) {
					ans.add(hostStripe);
				}
				return true;
			}
		});
		return ans;
	}

	@Override
	public boolean isMyJob(long docId) {
		return docId % (long) nBuddies == (long) myBuddyIndex;
	}

	@Override public int myBuddyIndex() { return myBuddyIndex; }

	@Override public int myDiskStripe() { return myDiskStripe; }

	@Override public int myHostStripe() { return myHostStripe; }

	@Override
	public int hostNameToStripe(String hostName) {
		if (!hostNameToStripe.containsKey(hostName)) {
			throw new IllegalArgumentException("Host name `" + hostName + "' not mapped to a stripe.");
		}
		return hostNameToStripe.get(hostName);
	}

	@Override
	public String hostStripeToName(int hostStripe) {
		if (!hostStripeToName.containsKey(hostStripe)) {
			throw new IllegalArgumentException("Host stripe " + hostStripe + " not mapped to a host.");
		}
		return hostStripeToName.get(hostStripe);
	}

	@Override
	public int hostToDiskStripe(int hostStripe) {
		if (!hostToDiskStripe.containsKey(hostStripe)) {
			throw new IllegalArgumentException("Host stripe " + hostStripe + " not associated with a disk stripe.");
		}
		return hostToDiskStripe.get(hostStripe);
	}

	@Override
	public File myTokenIndexRunDir() {
		return new File(config.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexRunPattern).replaceAll("\\$\\{HostName\\}", myHostName));
	}

	@Override
	public File mySipIndexRunDir() {
		return new File(config.getString(iitb.CSAW.Index.PropertyKeys.sipIndexRunPattern).replaceAll("\\$\\{HostName\\}", myHostName));
	}

	@Override
	public File getTmpDir(int aHostStripe) {
		return new File(config.getString(tmpDirKey).replaceAll("\\$\\{HostName\\}", hostStripeToName(aHostStripe)));
	}
	
	private int chooseHostFromDiskStripe(int aDiskStripe) {
		return (myDiskStripe == aDiskStripe)? myHostStripe : buddyHostStripes(aDiskStripe).getInt(0);
	}

	@Override
	public URI corpusDir(int aDiskStripe) throws URISyntaxException {
		final int aHostStripe = chooseHostFromDiskStripe(aDiskStripe);
		final String aHostName = hostStripeToName(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Corpus.Webaroo.PropertyKeys.corpusPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe;
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public URI tokenIndexHostDir(int aHostStripe) throws URISyntaxException {
		final String aHostName = hostStripeToName(aHostStripe);
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexHostPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe + File.separator + aHostStripe;
		ensureHostDir(aHostStripe, aPath);
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public URI sipIndexHostDir(int aHostStripe) throws URISyntaxException {
		final String aHostName = hostStripeToName(aHostStripe);
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.sipIndexHostPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe + File.separator + aHostStripe;
		ensureHostDir(aHostStripe, aPath);
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public File tokenIndexHostMirrorDir(int aHostStripe) {
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		return new File(config.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexHostPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + aDiskStripe + File.separator + aHostStripe);
	}

	@Override
	public File sipIndexHostMirrorDir(int aHostStripe) {
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		return new File(config.getString(iitb.CSAW.Index.PropertyKeys.sipIndexHostPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + aDiskStripe + File.separator + aHostStripe);
	}

	@Override
	public URI tokenIndexDiskDir(int aDiskStripe) throws URISyntaxException {
		final int aHostStripe = chooseHostFromDiskStripe(aDiskStripe);
		final String aHostName = hostStripeToName(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexDiskPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe;
		ensureDiskDir(aDiskStripe, aPath);
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public URI sipIndexDiskDir(int aDiskStripe) throws URISyntaxException {
		final int aHostStripe = chooseHostFromDiskStripe(aDiskStripe);
		final String aHostName = hostStripeToName(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.sipIndexDiskPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe;
		ensureDiskDir(aDiskStripe, aPath);
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public URI sipIndexDiskRemoteDir(int aHostStripe) throws URISyntaxException {
		final String aHostName = hostStripeToName(aHostStripe);
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.sipIndexDiskPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe;
		return new URI(scheme, aHostName, aPath, null);
	}

	@Override
	public URI tokenIndexDiskRemoteDir(int aHostStripe) throws URISyntaxException {
		final String aHostName = hostStripeToName(aHostStripe);
		final int aDiskStripe = hostToDiskStripe(aHostStripe);
		final String aPath = config.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexDiskPattern).replaceAll("\\$\\{HostName\\}", aHostName) + File.separator + aDiskStripe;
		return new URI(scheme, aHostName, aPath, null);
	}
	
	private void ensureDiskDir(int aDiskStripe, String path) {
		if (aDiskStripe != myDiskStripe()) return;
		final File d1 = new File(path);
		if (!d1.isDirectory()) {
			final boolean rc1 = d1.mkdir();
			logger.warn((rc1? "Created " : "Failed to create ") + d1);
		}
	}
	
	/**
	 * @param aHostStripe
	 * @param path of the form /path/to/diskStripe/HostStripe
	 */
	private void ensureHostDir(int aHostStripe, String path) {
		if (aHostStripe != myHostStripe()) return;
		final File d2 = new File(path);
		if (d2.isDirectory()) return;
		final File d1 = d2.getParentFile();
		if (!d1.isDirectory()) {
			final boolean rc1 = d1.mkdir();
			logger.warn((rc1? "Created " : "Failed to create ") + d1);
		}
		final boolean rc2 = d2.mkdir();
		logger.warn((rc2? "Created " : "Failed to create ") + d2);
	}

	/**
	 * @param args [0]=/path/to/config/file [1]=/path/to/log/file
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		WebarooStripeManager sm = new WebarooStripeManager(config);
		config.save(System.out);
		System.out.println(sm.isMyJob(new File(" out_data_")));
		System.out.println(sm.isMyJob(new File("out_data_11012+foo")));
		System.out.println(sm.docIdToPrefix(5654093));
		System.out.println(sm.docIdToPrefix(112120690));
		System.out.println(sm.numDiskStripes() + " " + sm.numHostStripes());
	}
}
