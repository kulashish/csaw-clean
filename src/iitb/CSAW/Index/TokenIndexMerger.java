package iitb.CSAW.Index;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Corpus.Webaroo.TokenIndexBuilder;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.BitStreamIndex;
import it.unimi.dsi.mg4j.index.CompressionFlags;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.dsi.mg4j.tool.Combine;
import it.unimi.dsi.mg4j.tool.Merge;
import it.unimi.dsi.mg4j.tool.ScanBatch;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.SemiExternalGammaList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

/**
 * Merges {@link ScanBatch} runs into a single index.
 * @author soumen
 */
public class TokenIndexMerger extends BaseIndexMerger {
	/**
	 * @param args [0]=/path/to/properties [1]=/path/to/log [2]=opcode
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		final TokenIndexMerger im = new TokenIndexMerger(config);
		if (args.length > 2) {
			if (args[2].equals("host")) {
				im.mergeWithinHostSequentially();
			}
			else if (args[2].equals("buddy")) {
				im.syncPullToLeader(im.stripeManager, im.field);
				im.mergeHostsToDiskStripe();
				im.writeTermMap();
				im.syncPushToBuddies(im.stripeManager, im.field);
			}
			else if (args[2].equals("bij")) {
				im.writeBijection();
			}
		}
	}

	enum Suffix { frequencies, globcounts, index, offsets, posnumbits, properties, /* sizes, */ terms, };
	static final int fanIn = 6;

	final Config config;
	final AStripeManager stripeManager;
	final Logger logger = Logger.getLogger(getClass());
	final Field field = Field.token;
	final String fieldName = field.toString();

	/** Per host area (typically on local disk) where runs have been stored by {@link TokenIndexBuilder}. */
	final File tokenIndexRunDir;
	/** Where, on a shared file system, to place a single index per host by merging local runs. */
	final File tokenIndexHostDir;
	/** Where to place the merged index for this disk stripe, on a shared file system. */
	final File tokenIndexDiskDir;
	/** Temporary working space to be cleared out before quitting. */
	final File tmpDir;
	
	final int nThreads;
	final AtomicInteger runCounter;
	
	TokenIndexMerger(Config config) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, ConfigurationException, URISyntaxException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		
		tokenIndexRunDir = stripeManager.myTokenIndexRunDir();
		final URI tokenIndexHostUri = stripeManager.tokenIndexHostDir(stripeManager.myHostStripe());
		assert tokenIndexHostUri.getHost().equals(stripeManager.myHostName());
		tokenIndexHostDir = new File(tokenIndexHostUri.getPath());
		if (!tokenIndexHostDir.isDirectory() && !tokenIndexHostDir.mkdir()) {
			throw new IllegalArgumentException("Cannot create/access directory " + tokenIndexHostDir);
		}
		final URI tokenIndexDiskUri = stripeManager.tokenIndexDiskDir(stripeManager.myDiskStripe());
		assert tokenIndexDiskUri.getHost().equals(stripeManager.myHostName());
		tokenIndexDiskDir = new File(tokenIndexDiskUri.getPath());
		tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		if (!tmpDir.isDirectory() || tmpDir.list().length > 0) {
			throw new IllegalArgumentException("Bad temporary space " + tmpDir);
		}
		
		nThreads = config.getInt(Config.nThreadsKey);
		runCounter = new AtomicInteger();
	}
	
	String getBaseName(File dir, String field, int runno) throws IOException {
		return dir.getCanonicalPath() + File.separator + field + "@" + runno;
	}
	
	/**
	 * We will check for a completed batch by the presence of a file with one
	 * designated suffix (out of a fraternity of files with the same prefix
	 * and different suffixes. We will later convert it into a prefix form suited
	 * for merging.
	 * @param dir directory under which to look for candidate runs
	 * @param workList output where list of runs is stored 
	 */
	void locateCandidateRuns(File dir, boolean doRecurse, Collection<File> workList) {
		if (!dir.isDirectory()) {
			return;
		}
		boolean foundCandidate = false;
		for (File ifile : dir.listFiles()) {
			if (!ifile.isFile()) {
				continue;
			}
			Pattern pattern = Pattern.compile(field + "(_|\\@)(\\d+)\\." + Suffix.frequencies);
			Matcher matcher = pattern.matcher(ifile.getName());
			if (!matcher.matches()) {
				continue;
			}
			if (appearsComplete(ifile)) {
				workList.add(ifile);
				foundCandidate = true;
			}
		}
		if (foundCandidate || !doRecurse) {
			return; // if there is a candidate run name, no need to expand subdirectories
		}
		for (File idir : dir.listFiles()) {
			if (idir.isDirectory()) {
				locateCandidateRuns(idir, doRecurse, workList);
			}
		}
	}
	
	boolean appearsComplete(File slice) {
		final File dir = slice.getParentFile();
		final String name = slice.getName();
		final int lastDotPos = name.lastIndexOf('.');
		final String namePrefix = name.substring(0, lastDotPos);
		boolean ans = true;
		for (Suffix suffix : Suffix.values()) {
			final File test = new File(dir, namePrefix + "." + suffix);
			ans = ans && test.isFile() && test.canRead();
		}
		logger.trace(slice + " " + ans);
		return ans;
	}
	
	String[] getInputBaseNames(Collection<File> runFiles) throws IOException {
		final ArrayList<String> runBaseList = new ArrayList<String>();
		for (File runFile : runFiles) {
			final String runBase = runFile.getCanonicalPath();
			runBaseList.add(runBase.substring(0, runBase.lastIndexOf('.')));
		}
		return runBaseList.toArray(new String[]{});
	}
	
	/**
	 * If any input task hits an exception the whole array is ignored and null returned.
	 */
	String[] getInputBaseNamesFuture(Collection<Future<File>> runFutures) throws IOException, InterruptedException, ExecutionException {
		final ArrayList<String> runBaseList = new ArrayList<String>();
		for (Future<File> runFuture: runFutures) {
			final File runFile = runFuture.get();
			if (runFile == null) {
				logger.fatal("Error with " + runFuture + ": aborting");
				return null;
			}
			final String runBase = runFile.getCanonicalPath();
			runBaseList.add(runBase.substring(0, runBase.lastIndexOf('.')));
		}
		return runBaseList.toArray(new String[]{});
	}

	void getIndexSize(File repFile, MutableLong maxDocIdPlusOne, MutableLong nOccs) throws ConfigurationException, FileNotFoundException, IOException {
		final File repDir = repFile.getParentFile();
		final String propName = repFile.getName().replaceAll(DiskBasedIndex.FREQUENCIES_EXTENSION, DiskBasedIndex.PROPERTIES_EXTENSION);
		final File propFile = new File(repDir, propName);
		it.unimi.dsi.util.Properties prop = new it.unimi.dsi.util.Properties(propFile);
		final String sizeName = repFile.getName().replaceAll(DiskBasedIndex.FREQUENCIES_EXTENSION, DiskBasedIndex.SIZES_EXTENSION);
		final File sizeFile = new File(repDir, sizeName);
		final SemiExternalGammaList sizeSegl = new SemiExternalGammaList(new InputBitStream(sizeFile));
		final int md1 = prop.getInt(Index.PropertyKeys.DOCUMENTS), md2 = sizeSegl.size();
		if (md1 != md2) {
			logger.info(propFile + " " + prop.getLong(Index.PropertyKeys.OCCURRENCES) + " " + (md1 == md2));
		}
	}
	
	/**
	 * Single threaded version.
	 * @throws Exception 
	 */
	void mergeWithinHostSequentially() throws  Exception {
		final HashSet<File> inputRuns = new HashSet<File>();
		locateCandidateRuns(tokenIndexRunDir, true, inputRuns);
//		for (File repFile : inputRuns) {
//			getIndexSize(repFile, null, null);
//		}
		LinkedList<File> workList = new LinkedList<File>(inputRuns);
		logger.warn(workList.size() + " input runs to merge");
		while(workList.size() > 0) {
			final ArrayList<File> subWorkList = new ArrayList<File>();
			for (int fx = 0; fx < fanIn && workList.size() > 0; ++fx) {
				subWorkList.add(workList.removeFirst());
			}
			final String[] inputBasenames = getInputBaseNames(subWorkList);
			// generate output run name
			final String outBasename;
			if (workList.size() == 0) {
				outBasename = tokenIndexHostDir + File.separator + field;
			}
			else {
				final int runNo = runCounter.getAndIncrement();
				outBasename = getBaseName(tmpDir, fieldName, runNo);
			}
			logger.info(" Merge " + subWorkList + " into " + outBasename);
			// invoke MG4J merge
			final boolean interleaved = false, skips = true;
			new Merge(outBasename, inputBasenames, false, Combine.DEFAULT_BUFFER_SIZE, CompressionFlags.DEFAULT_STANDARD_INDEX, interleaved, skips, BitStreamIndex.DEFAULT_QUANTUM, BitStreamIndex.DEFAULT_HEIGHT, SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL).run();
			// no point saving termmap here
			for (File mergedRun : subWorkList) {
				if (!inputRuns.contains(mergedRun)) {
					deleteRunFiles(mergedRun);
				}
			}
			System.gc();
			if (workList.size() == 0) {
				logger.info("Merge root is " + outBasename);
				break;
			}
			else {
				workList.addLast(new File(outBasename + "." + Suffix.frequencies));
			}
		}
	}

	/**
	 * Delete runFile and all its siblings.
	 * @param representativeRunFile
	 * @throws IOException
	 */
	void deleteRunFiles(File representativeRunFile) throws IOException {
		final File rrDir = representativeRunFile.getParentFile();
		final String delName = representativeRunFile.getName();
		final String delBase = delName.substring(0, delName.lastIndexOf('.'));
		for (File delFile : rrDir.listFiles()) {
			if (!delFile.getName().startsWith(delBase + ".")) {
				continue;
			}
			logger.warn("Deleting " + delFile);
			final boolean wasDeleted = delFile.delete(); 
			if (!wasDeleted) {
				logger.warn("Cannot delete " + delFile.getCanonicalPath());
			}
		}
	}

	/**
	 * To be called only by the leader of a buddy set.
	 * Inputs one run per buddy (including itself) and outputs
	 * one index for the disk stripe.
	 */
	void mergeHostsToDiskStripe() throws Exception {
		if (stripeManager != null && stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Should run this only on buddy master.");
			return;
		}
		final ReferenceArrayList<File> allBuddyDirs = new ReferenceArrayList<File>();
		final File parent = tokenIndexHostDir.getParentFile();
		// any purely numeric subdir is a buddy dir, blech
		for (File possibleBuddyDir : parent.listFiles()) {
			if (possibleBuddyDir.getName().matches("\\d+")) {
				allBuddyDirs.add(possibleBuddyDir);
			}
		}
		final ReferenceArrayList<File> buddyRunFiles = new ReferenceArrayList<File>();
		for (File buddyDir : allBuddyDirs) {
//			locateCandidateRuns(buddyDir, false, buddyRunFiles); // don't recurse
			// 2010-11-02 We don't need to locate runs because per-host merging saves to a fixed file name
			final File freqFile = new File(buddyDir, field + DiskBasedIndex.FREQUENCIES_EXTENSION);
			if (freqFile.canRead()) {
				buddyRunFiles.add(freqFile);
			}
		}
		logger.info("Buddy runs to be merged " + buddyRunFiles);
		final String[] buddyBaseNames = getInputBaseNames(buddyRunFiles);
		if (tokenIndexDiskDir.listFiles().length != 0) {
			logger.warn("Target directory " + tokenIndexDiskDir + " is not empty, quitting.");
			return;
		}
		final String outBaseName = tokenIndexDiskDir.getCanonicalPath() + File.separator + field;
		// invoke MG4J merge
		final boolean interleaved = false, skips = true;
		final Merge merge = new Merge(outBaseName, buddyBaseNames, false, Combine.DEFAULT_BUFFER_SIZE, CompressionFlags.DEFAULT_STANDARD_INDEX, interleaved, skips, BitStreamIndex.DEFAULT_QUANTUM, BitStreamIndex.DEFAULT_HEIGHT, SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL);
		merge.writeSizes = false;
		merge.run();
	}
	
	void writeTermMap() throws Exception {
		final String baseNameField = new File(tokenIndexDiskDir, field.toString()).toString();
		logger.info( "Creating term map from " +  baseNameField + DiskBasedIndex.TERMS_EXTENSION);
		final File dumpStreamFile = new File(tokenIndexDiskDir, field + DiskBasedIndex.TERMDUMP_EXTENSION);
		final FileLinesCollection terms = new FileLinesCollection( baseNameField + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" );
		ImmutableExternalPrefixMap termMap = new ImmutableExternalPrefixMap(terms, dumpStreamFile.getAbsolutePath());
		logger.info("Saving term map to " + baseNameField + DiskBasedIndex.TERMMAP_EXTENSION + " and " + dumpStreamFile);
		BinIO.storeObject(termMap, baseNameField + DiskBasedIndex.TERMMAP_EXTENSION );
		logger.info( "Done." );
	}

	/**
	 * Typically used only for the small reference corpus.  The vocabulary of
	 * the payload corpus is usually too large to store a bijection in RAM.
	 * @throws IOException 
	 */
	void writeBijection() throws IOException {
		final String baseNameField = new File(tokenIndexDiskDir, field.toString()).toString();
		final File termsFile = new File(tokenIndexDiskDir, field + DiskBasedIndex.TERMS_EXTENSION);
		StringIntBijection termsBij = new StringIntBijection(new FileLinesCollection(termsFile.getCanonicalPath(), "UTF-8"));
		final String outPath = baseNameField + iitb.CSAW.Index.PropertyKeys.tokenBijExtension;
		BinIO.storeObject(termsBij, outPath);
		logger.info( "Saved term bijection to " + outPath);
	}
}
