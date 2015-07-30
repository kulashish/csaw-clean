package iitb.CSAW.Index;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.RecordDigest;
import iitb.CSAW.Utils.RemoteData;
import iitb.CSAW.Utils.Sort.BitExternalMergeSort;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.util.Properties;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

/**
 * Merges all runs of one host. Runs as buddy leader to merge all buddies into
 * one index per disk stripe. Scans and computes seek offsets and SHA 
 * checksums for all ent and type keys. Shared across variants of SIP indices.
 * @author soumen
 *
 * @param <SD>
 */
public class SipIndexMerger<SD extends ISipDocument<SD>> extends BaseIndexMerger {
	static final int FAST_BUFFER_SIZE = 16*(1<<20);
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final AStripeManager stripeManager;
	final ACatalog catalog;
	final File sipIndexRunDir, tmpDir;
	final int nThreads;
	final Class<SD> sdType;
	
	final ArrayList<File> inputRunFiles = new ArrayList<File>();
	
	public SipIndexMerger(Config config, Class<SD> sdType) throws Exception {
		this.config = config;
		this.stripeManager = AStripeManager.construct(config);
		this.catalog = ACatalog.construct(config);
		this.sipIndexRunDir = stripeManager.mySipIndexRunDir();
		this.tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		this.nThreads = config.getInt(Config.nThreadsKey);
		this.sdType = sdType;
	}
	
	public void main(String[] args) throws Exception {
		for (int ax = 2; ax < args.length; ++ax) {
			if (args[ax].equals("host")) { // all hosts
				mergeWithinHost(Field.ent);
				aggregateRunShaToHostSha(Field.ent);
				mergeWithinHost(Field.type);
				aggregateRunShaToHostSha(Field.type);
			}
			else if (args[ax].equals("buddy")) { // only buddy leaders
				syncPullToLeader(stripeManager, Field.ent);
				mergeHostsToDiskStripe(Field.ent);
				prepareDiskStripeDigests(Field.ent);		
				aggregateHostShaToDiskSha(Field.ent);
				mergeHostsToDiskStripe(Field.type);
				prepareDiskStripeDigests(Field.type);
				aggregateHostShaToDiskSha(Field.type);
				syncPushToBuddies(stripeManager, Field.ent);
			}
			else if (args[ax].equals("global")) { // all hosts
				prepareGlobalCounts();
				prepareGlobalTotals();
			}
			else if (args[ax].equals("checksum")) { // all hosts
				verifySipSha();
			}
		}
	}
	
	/**
	 * Turn local frequency files into global frequencies.
	 * For ent and type we expect all gamma arrays to be the same size,
	 * and be able to hold them in RAM uncompressed.
	 * @param field
	 * @throws Exception 
	 */
	void prepareGlobalCounts(Field field, String inExt, String outExt) throws Exception {
		logger.info("prepareGlobalCounts " + field + " " + inExt + " " + outExt);
		assert field == Field.ent || field == Field.type;
		final int numElems = field == Field.ent? catalog.numEnts() : catalog.numCats();
		final long[] myElems = new long[numElems], otherElems = new long[numElems];
		final URI mySipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		final File myLocalCountsFile = new File(mySipIndexDiskUri.getPath(), field + inExt);
		readArrayGamma(myLocalCountsFile, myElems);
		for (int otherDiskStripe = 0; otherDiskStripe < stripeManager.numDiskStripes(); ++otherDiskStripe) {
			if (otherDiskStripe == stripeManager.myDiskStripe()) {
				continue;
			}
			final URI otherSipIndexDiskUri = stripeManager.sipIndexDiskDir(otherDiskStripe);
			final String otherPath = otherSipIndexDiskUri.getPath() + File.separator + field + inExt;
			final URI otherSipIndexCountUri = new URI(otherSipIndexDiskUri.getScheme(), otherSipIndexDiskUri.getHost(), otherPath, "");
			readArrayGamma(otherSipIndexCountUri, otherElems);
			for (int cx = 0; cx < numElems; ++cx) {
				myElems[cx] += otherElems[cx];
			}
		}
		final File myGlobalCountsFile = new File(mySipIndexDiskUri.getPath(), field + outExt);
		writeArrayGamma(myGlobalCountsFile, myElems);
	}
	
	void prepareGlobalCounts() throws Exception {
		prepareGlobalCounts(Field.ent, iitb.CSAW.Index.PropertyKeys.sipCompactLocalCfExtension, iitb.CSAW.Index.PropertyKeys.sipCompactGlobalCfExtension);
		prepareGlobalCounts(Field.ent, iitb.CSAW.Index.PropertyKeys.sipCompactLocalDfExtension, iitb.CSAW.Index.PropertyKeys.sipCompactGlobalDfExtension);
		prepareGlobalCounts(Field.type, iitb.CSAW.Index.PropertyKeys.sipCompactLocalCfExtension, iitb.CSAW.Index.PropertyKeys.sipCompactGlobalCfExtension);
		prepareGlobalCounts(Field.type, iitb.CSAW.Index.PropertyKeys.sipCompactLocalDfExtension, iitb.CSAW.Index.PropertyKeys.sipCompactGlobalDfExtension);
	}
	
	/**
	 * As with the {@link Field#token} index, reads
	 * per-disk-stripe {@link Properties} files, aggregates occurrence and
	 * document counts, and writes back new keys.
	 * @throws Exception 
	 */
	void prepareGlobalTotals() throws Exception {
		mergeDocsOccsAcrossDiskStripes(stripeManager, Field.ent);
		mergeDocsOccsAcrossDiskStripes(stripeManager, Field.type);
	}

	void prepareDiskStripeDigests(Field field) throws IOException, ConfigurationException, URISyntaxException, InstantiationException, IllegalAccessException {
		switch (field) {
		case ent:
			prepareDiskStripeDigests(field, catalog.numEnts());
			break;
		case type:
			prepareDiskStripeDigests(field, catalog.numCats());
			break;
		}
	}
	
	/**
	 * Scans compact postings and saves a seek offset file, a corpus frequency
	 * file, and a document frequency file.
	 * <b>Note:</b> gamma-encoded number list elements are shifted by +1,
	 * and 0 is reserved for null.
	 * Run only on buddy leaders.
	 * @throws ConfigurationException 
	 * @throws URISyntaxException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	void prepareDiskStripeDigests(Field prefix, int numKeys) throws IOException, ConfigurationException, URISyntaxException, InstantiationException, IllegalAccessException {
		if (stripeManager.myBuddyIndex() != 0) {
			throw new IllegalArgumentException("Run only on buddy leader");
		}
		
		final long[] keyToSeek = new long[numKeys];
		final int[] keyToDf = new int[numKeys];
		final long[] keyToCf = new long[numKeys];
		final URI sipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		final File diskRunFile = new File(sipIndexDiskUri.getPath(), prefix + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
		logger.info("Digesting " + diskRunFile);
		final InputBitStream diskRunIbs = new InputBitStream(diskRunFile);
		final ISipDocument<SD> ssd = sdType.newInstance();
		int maxDoc = 0, df = 0, prevKey = -1, prevDocId = -1;
		long numOcc = 0, cf = 0;
		for (;;) {
			final long seek = diskRunIbs.readBits();
			try {
				ssd.load(diskRunIbs);
			}
			catch (EOFException eofx) {
				break;
			}
			maxDoc = Math.max(maxDoc, ssd.docId());
			numOcc += ssd.nPosts(); // spanLeft.size();
			assert ssd.entOrCatId() > prevKey || ssd.docId() > prevDocId;
			if (ssd.entOrCatId() > prevKey) {
				keyToSeek[ssd.entOrCatId()] = GAMMA_OFFSET + seek;
				if (prevKey != -1) {
					keyToDf[prevKey] = GAMMA_OFFSET + df;
					keyToCf[prevKey] = GAMMA_OFFSET + cf;
					df = 0;
					cf = 0;
				}
			}
			++df;
			cf += ssd.nPosts(); // spanLeft.size();
			prevKey = ssd.entOrCatId();
			prevDocId = ssd.docId();
		}
		if (prevKey != -1) {
			keyToDf[prevKey] = GAMMA_OFFSET + df;
			keyToCf[prevKey] = GAMMA_OFFSET + cf;
		}
		diskRunIbs.close();
		writeArrayGamma(new File(sipIndexDiskUri.getPath(), prefix + iitb.CSAW.Index.PropertyKeys.sipCompactSeekExtension), keyToSeek);
		writeArrayGamma(new File(sipIndexDiskUri.getPath(), prefix + iitb.CSAW.Index.PropertyKeys.sipCompactLocalCfExtension), keyToCf);
		writeArrayGamma(new File(sipIndexDiskUri.getPath(), prefix + iitb.CSAW.Index.PropertyKeys.sipCompactLocalDfExtension), keyToDf);
		
		Properties properties = new Properties();
		properties.addProperty(Index.PropertyKeys.DOCUMENTS, 1+maxDoc);
		properties.addProperty(Index.PropertyKeys.OCCURRENCES, numOcc);
		properties.save(new File(sipIndexDiskUri.getPath(), prefix + DiskBasedIndex.PROPERTIES_EXTENSION));
	}
	
	void writeArrayGamma(File gammaFile, int[] array) throws IOException {
		final OutputBitStream gammaObs = new OutputBitStream(gammaFile);
		for (int v : array) {
			gammaObs.writeGamma(v);
		}
		gammaObs.close();
	}

	void writeArrayGamma(File gammaFile, long[] array) throws IOException {
		final OutputBitStream gammaObs = new OutputBitStream(gammaFile);
		for (long v : array) {
			gammaObs.writeLongGamma(v);
		}
		gammaObs.close();
	}
	
	void readArrayGamma(File gammaFile, long[] array) throws IOException {
		logger.info("Reading " + gammaFile);
		final InputStream gammaStream = new FileInputStream(gammaFile);
		readArrayGamma(gammaStream, array);
		gammaStream.close();
	}
	
	void readArrayGamma(URI gammaUri, long[] array) throws Exception {
		logger.info("Reading " + gammaUri);
		final RemoteData rd = new RemoteData(gammaUri.getHost());
		try {
			final InputStream gammaStream = rd.getRemoteFileInputStream(gammaUri.getPath());
			readArrayGamma(gammaStream, array);
			gammaStream.close();
		}
		finally {
			rd.close();
		}
	}
	
	void readArrayGamma(InputStream gammaStream, long[] array) throws IOException {
		Arrays.fill(array, 0);
		final InputBitStream gammaIbs = new InputBitStream(gammaStream);
		int ofs = 0;
		try {
			for (;;) {
				final long count = gammaIbs.readLongGamma();
				array[ofs++] = count;
			}
		}
		catch (EOFException eofx) {
			if (ofs != array.length) {
				logger.warn("Gamma stream has " + ofs + " numbers but array.length=" + array.length);
			}
		}
		finally {
			gammaIbs.close();
		}
	}
	
	/**
	 * Run only on buddy leaders.  Merges buddy host runs to one run per disk stripe.
	 * @throws URISyntaxException 
	 */
	void mergeHostsToDiskStripe(Field field) throws InterruptedException, ExecutionException, IOException, InstantiationException, IllegalAccessException, RuntimeException, URISyntaxException {
		if (stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Can only run on buddy leader");
			return;
		}
		final URI mySipIndexHostUri = stripeManager.sipIndexHostDir(stripeManager.myHostStripe());
		assert mySipIndexHostUri.getHost().equals(stripeManager.myHostName());
		final File mySipIndexHostDir = new File(mySipIndexHostUri.getPath());
		final File parentOfSipIndexHostDirs = mySipIndexHostDir.getParentFile();
		/*
		 * Host indices of mirrored buddies will be under parent defined above.
		 * Any purely numeric subdir is a buddy dir, blech.
		 */
		inputRunFiles.clear();
		for (File candidateSipIndexHostDir : parentOfSipIndexHostDirs.listFiles()) {
			if (candidateSipIndexHostDir.isDirectory() && candidateSipIndexHostDir.getName().matches("\\d+")) {
				collectRunFiles(candidateSipIndexHostDir, field.toString(), inputRunFiles);
			}
		}
		final URI mySipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		assert mySipIndexDiskUri.getHost().equals(stripeManager.myHostName());
		final File sipDiskStripeFile = new File(mySipIndexDiskUri.getPath(), field + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
		logger.info(inputRunFiles + " --> " + sipDiskStripeFile);
		mergeGeneric(field, sipDiskStripeFile);
	}
	
	/**
	 * Run on all hosts.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	void mergeWithinHost(Field field) throws IOException, InterruptedException, ExecutionException, InstantiationException, IllegalAccessException, URISyntaxException {
		inputRunFiles.clear();
		collectRunFiles(sipIndexRunDir, field.toString(), inputRunFiles);
		final URI sipIndexHostUri = stripeManager.sipIndexHostDir(stripeManager.myHostStripe());
		final File sipHostOutFile = new File(sipIndexHostUri.getPath(), field.toString() + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
		mergeGeneric(field, sipHostOutFile);
	}
	
	private void mergeGeneric(Field field, File sipHostOutFile) throws InstantiationException, IllegalAccessException, RuntimeException, IOException, InterruptedException, ExecutionException {
		final BitExternalMergeSort<SD> bems = new BitExternalMergeSort<SD>(sdType, tmpDir);
		bems.mergeUsingHeapLimitedFanIn(inputRunFiles, sipHostOutFile);
	}

	void collectRunFiles(File indexDir, String prefix, Collection<File> runFiles) {
		for (File candFile : indexDir.listFiles()) {
			final String candName = candFile.getName();
			if (candName.startsWith(prefix) && candName.endsWith(iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension)) {
				runFiles.add(candFile);
			}
		}
	}
	
	private void aggregateHostShaToDiskSha(Field field) throws NoSuchAlgorithmException, IOException, ClassNotFoundException, URISyntaxException {
		if (stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Can only run on buddy leader");
			return;
		}
		final URI mySipIndexHostUri = stripeManager.sipIndexHostDir(stripeManager.myHostStripe());
		final File mySipIndexHostDir = new File(mySipIndexHostUri.getPath());
		final File parentOfSipIndexHostDirs = mySipIndexHostDir.getParentFile();
		final RecordDigest pd = new RecordDigest();
		final byte[] allHostSha = new byte[pd.getDigestLength()];
		for (File candidateSipIndexHostDir : parentOfSipIndexHostDirs.listFiles()) {
			if (candidateSipIndexHostDir.isDirectory() && candidateSipIndexHostDir.getName().matches("\\d+")) {
				collectSha(candidateSipIndexHostDir, field, allHostSha);
			}
		}
		final URI mySipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		assert mySipIndexDiskUri.getHost().equals(stripeManager.myHostName());
		final File diskShaFile = new File(mySipIndexDiskUri.getPath(), field + iitb.CSAW.Index.PropertyKeys.sipCompactShaExtension);
		BinIO.storeBytes(allHostSha, diskShaFile);
	}
	
	private void collectSha(File dir, Field field, byte[] allSha) throws IOException, ClassNotFoundException {
		int nSha = 0;
		for (File shaFile : dir.listFiles()) {
			if (!shaFile.getName().startsWith(field.toString())) continue;
			if (!shaFile.getName().endsWith(iitb.CSAW.Index.PropertyKeys.sipCompactShaExtension)) continue;
			logger.trace(shaFile);
			byte[] runSha = BinIO.loadBytes(shaFile);
			for (int bx = 0; bx < runSha.length; ++bx) {
				allSha[bx] ^= runSha[bx];
			}
			++nSha;
		}
		logger.debug("Collected " + nSha + " checksum files");
	}

	private void aggregateRunShaToHostSha(Field field) throws NoSuchAlgorithmException, IOException, ClassNotFoundException, URISyntaxException {
		final RecordDigest pd = new RecordDigest();
		byte[] allRunSha = new byte[pd.getDigestLength()];
		final File sipRunDir = stripeManager.mySipIndexRunDir();
		collectSha(sipRunDir, field, allRunSha);
		final String mySipIndexHostPath = stripeManager.sipIndexHostDir(stripeManager.myHostStripe()).getPath();
		final File hostShaFile = new File(mySipIndexHostPath, field + iitb.CSAW.Index.PropertyKeys.sipCompactShaExtension);
		BinIO.storeBytes(allRunSha, hostShaFile);
		logger.info("Aggregated checksum files from " + sipRunDir + " into " + hostShaFile);
	}
	
	private void verifySipSha() throws Exception {
		final URI sipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		logger.info("Verifying " + sipIndexDiskUri);
		verifySipSha(sipIndexDiskUri.getPath());
	}
	
	/**
	 * Given a directory with paired SIP files and their checksums, verifies
	 * the SIP files. 
	 * @param sipName
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	private void verifySipSha(String sipName) throws IOException, NoSuchAlgorithmException, DigestException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		final File sipDir = new File(sipName); // dir with sip files
		if (!sipDir.isDirectory()) {
			return;
		}
		for (File sipFile : sipDir.listFiles()) {
			if (!sipFile.getName().endsWith(iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension)) continue;
			logger.info("Scanning " + sipFile);
			RecordDigest pd = new RecordDigest();
			final InputBitStream csipIbs = new InputBitStream(sipFile);
			final ISipDocument<SD> ssd = sdType.newInstance();
			int minKey = Integer.MAX_VALUE, maxKey = Integer.MIN_VALUE;
			for (;;) {
				try {
					ssd.load(csipIbs);
					minKey = Math.min(minKey, ssd.entOrCatId());
					maxKey = Math.max(maxKey, ssd.entOrCatId());
					ssd.checkSum(pd);
				}
				catch (EOFException eofx) {
					break;
				}
			}
			csipIbs.close();
			logger.info(sipFile + " minKey=" + minKey + " maxKey=" + maxKey);
			final byte[] newCheckSum = pd.getDigest();
			final File shaFile = SipDocumentBuilder.runToShaFile(sipFile);
			if (shaFile.canRead()) {
				final byte[] oldCheckSum = BinIO.loadBytes(shaFile);
				final boolean matched = ByteBuffer.wrap(oldCheckSum, 0, oldCheckSum.length).equals(ByteBuffer.wrap(newCheckSum, 0, newCheckSum.length));
				final ByteArrayList newCheckSumBal = new ByteArrayList(newCheckSum);
				final ByteArrayList oldCheckSumBal = new ByteArrayList(oldCheckSum);
				final boolean matchedBal = newCheckSumBal.equals(oldCheckSumBal);
				logger.info(sipFile + " checksum " + matched + "/" + matchedBal);
				assert matched == matchedBal;
				if (!matched || !matchedBal) {
					logger.fatal(sipFile + " failed checksum");
					break;
				}
			}
		} // for files
	}
}
