package iitb.CSAW.Spotter;

import gnu.trove.TLongIntHashMap;
import gnu.trove.TLongIntIterator;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.LongIntInt;
import iitb.CSAW.Utils.RangeInputStream;
import iitb.CSAW.Utils.ReusableByteArrayDataOutput;
import iitb.CSAW.Utils.Sort.ExternalMergeSort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

/**
 * Support for sorting and scattering files of {@link ContextRecordCompact}.
 * There is no longer support for {@link ContextRecord}.
 * @author soumen
 */
public class ContextBase {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log
	 * [2..]=opcode in {sort, index, scan, scramble}, order is important
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		TokenCountsReader refTcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		for (int ac = 2; ac < args.length; ++ac) {
			ContextBase cb = null;
			if (args[ac].equals("sort")) {
				cb = new ContextBase(conf, refTcr);
				cb.sortContexts();
			}
			else if (args[ac].equals("index")) {
				cb = new ContextBase(conf, refTcr);
				cb.indexSortedContexts();
			}
			else if (args[ac].equals("scan")) {
				cb = new ContextBase(conf, refTcr);
				cb.scanContextsLeafByLeaf();
			}
			else if (args[ac].equals("scramble")) {
				cb = new ContextBase(conf, refTcr);
				cb.scrambleContexts(new File(args[2]));
			}
			if (cb != null) {
				cb.close();
			}
		}
	}
	
	final Logger logger = Logger.getLogger(getClass());
	protected final Config conf;
	final AStripeManager stripeManager;
	final File tmpDir;
	protected final MentionTrie trie;
	final ACatalog catalog;
	final TermProcessor tp;
	/** Token counts collected over reference corpus. */
	final TokenCountsReader refTcr;
	final DocumentSpotter spotter;
	final long[] leafIdToBlockEndOffset;
	
	/** Directory where context collectors save {@link ContextRecordCompact}. */
	final File ctxDir2;
	
	// pre-compact
	public static final String posContextFileName = "Pos.dat";
	public static final String negContextFileName = "Neg.dat";
	
	// compact
	public static final String compactContextFileName = "ContextCompact.dat";
	public static final String compactContextIndexName = "ContextCompact.idx";
	final File compactIndexFile, compactContextFile;
	
	// compact L-E-F keys moved to L-E-F-M class	
	// compact L-F-E keys moved to L-F-E-M base class

	/**
	 * @param conf
	 * @param refTcr collected over reference corpus
	 */
	public ContextBase(Config conf, TokenCountsReader refTcr) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, ConfigurationException {
		this.conf = conf;

		stripeManager = AStripeManager.construct(conf);
		tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		trie = MentionTrie.getInstance(conf);
		catalog = ACatalog.construct(conf);
		tp = DefaultTermProcessor.construct(conf);
		this.refTcr = refTcr;
		spotter = new DocumentSpotter(conf, this.refTcr);
		
		ctxDir2 = new File(conf.getString(PropertyKeys.contextBaseDirKey2));
		
		compactIndexFile = new File(ctxDir2, compactContextIndexName);
		compactContextFile = new File(ctxDir2, compactContextFileName);
		if (compactIndexFile.exists()) {
			if (compactIndexFile.lastModified() < compactContextFile.lastModified()) {
				throw new IllegalStateException(compactIndexFile + " is older than " + compactContextFile);
			}
			leafIdToBlockEndOffset = BinIO.loadLongs(compactIndexFile);
			logger.info("Loaded index " + compactIndexFile);
		}
		else {
			leafIdToBlockEndOffset = null;
		}
	}
	
	void close() throws IOException {
		refTcr.close();
	}
	
	/**
	 * Some learners (esp. online learners) prefer to get training instances in random order.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 */
	void scrambleContexts(File scrambledContextFile) throws IOException, InstantiationException, IllegalAccessException {
		Comparator<ContextRecordCompact> rc = new ContextRecordCompact.HashedComparator();
		final File tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		ExternalMergeSort<ContextRecordCompact> ems = new ExternalMergeSort<ContextRecordCompact>(ContextRecordCompact.class, rc, false, tmpDir);
		ems.runFanIn(compactContextFile, scrambledContextFile);
	}
	
	void sortContexts() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		if (compactIndexFile.exists() && compactIndexFile.lastModified() >= compactContextFile.lastModified()) {
			logger.info(compactIndexFile + " up to date, not sorting " + compactContextFile);
			return;
		}
		final File tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		ExternalMergeSort<ContextRecordCompact> ems = new ExternalMergeSort<ContextRecordCompact>(ContextRecordCompact.class, new ContextRecordCompact(), false, tmpDir);
		ems.runFanIn(compactContextFile, compactContextFile);
	}
	
	void indexSortedContexts() throws IOException {
		if (compactIndexFile.exists() && compactIndexFile.lastModified() >= compactContextFile.lastModified()) {
			logger.info(compactIndexFile + " up to date, not indexing " + compactContextFile);
			return;
		}
		final ContextRecordCompact crc = new ContextRecordCompact(), prevCrc = new ContextRecordCompact();
		final DataInputStream crcDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(compactContextFile)));
		final ReusableByteArrayDataOutput crcDos = new ReusableByteArrayDataOutput();
		final long[] leafIdToBlockEndOffset = new long[trie.getNumLeaves()+1];
		Arrays.fill(leafIdToBlockEndOffset, Long.MIN_VALUE);
		final long size = compactContextFile.length();
		long last = System.currentTimeMillis(), bytesWritten = 0;
		ProgressLogger pl1 = new ProgressLogger(logger);
		pl1.logInterval = ProgressLogger.ONE_MINUTE;
		pl1.displayFreeMemory = true;
		pl1.start("Started indexing " + compactContextFile);
		for (;;) {
			try {
				crc.load(crcDis);
				if (prevCrc.trieLeaf >= 0 && crc.compare(prevCrc, crc) > 0) {
					logger.fatal(compactContextFile + " not sorted " + crc + " " + prevCrc);
					break;
				}
				if (prevCrc.trieLeaf >= 0 && prevCrc.trieLeaf < crc.trieLeaf) {
					logger.debug(prevCrc.trieLeaf + " --> " + bytesWritten);
					leafIdToBlockEndOffset[prevCrc.trieLeaf] = bytesWritten; 
				}
				crcDos.reset();
				crc.store(crcDos);
				bytesWritten += crcDos.longSize();
				if (System.currentTimeMillis() > last + ProgressLogger.ONE_MINUTE) {
					logger.info(100 * bytesWritten / size + "%");
					last = System.currentTimeMillis();
				}
				prevCrc.replace(crc);
				pl1.update();
			}
			catch (EOFException eofx) {
				logger.debug(prevCrc.trieLeaf + " --> " + bytesWritten);
				leafIdToBlockEndOffset[prevCrc.trieLeaf] = bytesWritten;
				break;
			}
		}
		pl1.stop("Finished indexing " + compactContextFile);
		pl1.done();
		crcDis.close();
		BinIO.storeLongs(leafIdToBlockEndOffset, compactIndexFile);
	}
	
	protected DataInputStream getLeafStream(int leaf) throws IOException {
		assert 0 <= leaf && leaf < leafIdToBlockEndOffset.length : "L" + leaf + " beyond range [0," + leafIdToBlockEndOffset.length + ")";
		final long leafBlockEnd = leafIdToBlockEndOffset[leaf];
		if (leafBlockEnd == Long.MIN_VALUE) {
			return null;
		}
		long leafBlockBegin = 0;
		for (int prevLeaf = leaf-1; prevLeaf >= 0; --prevLeaf) {
			if (leafIdToBlockEndOffset[prevLeaf] != Long.MIN_VALUE) {
				leafBlockBegin = leafIdToBlockEndOffset[prevLeaf];
				break;
			}
		}
		logger.debug("L" + leaf + " [" + leafBlockBegin + "," + leafBlockEnd + ")");
		return new DataInputStream(new BufferedInputStream(new RangeInputStream(compactContextFile, leafBlockBegin, leafBlockEnd)));
	}
	
	protected DataInputStream getAllLeafStream() throws IOException {
		return new DataInputStream(new BufferedInputStream(new FileInputStream(compactContextFile)));
	}
	
	/**
	 * Sample use of per-leaf scanner to be used for training annotators.
	 * @throws IOException
	 */
	void scanContextsLeafByLeaf() throws IOException {
		final ContextRecordCompact crc = new ContextRecordCompact(), prevCrc = new ContextRecordCompact();

		// first scan monolithic to get total record count
		ProgressLogger pl1 = new ProgressLogger(logger);
		long nRec1 = 0, nRec2 = 0;
		final DataInputStream allDis = new DataInputStream(new BufferedInputStream(new FileInputStream(compactContextFile)));
		pl1.start("Starting monolithic scan");
		prevCrc.trieLeaf = -1;
		for (;;) {
			try {
				crc.load(allDis);
				++nRec1;
				if (prevCrc.compare(prevCrc, crc) > 0) {
					throw new IllegalStateException("prevCrc=" + prevCrc + " crc=" + crc);
				}
				final IntList candEnts = trie.getSortedEntsNa(crc.trieLeaf);
				if (!candEnts.contains(crc.entId)) {
					throw new IllegalStateException(crc + " " + candEnts + " E" + crc.entId);
				}
				prevCrc.replace(crc);
			}
			catch (EOFException eofx) {
				break;
			}
		}
		allDis.close();
		pl1.stop("Finished monolithic scan");
		pl1.done();
		logger.info(nRec1 + " records");
		
		int nEmptyLeaves = 0;
		// read segment by segment
		ProgressLogger pl2 = new ProgressLogger(logger);
		pl2.expectedUpdates = trie.getNumLeaves();
		pl2.displayFreeMemory = true;
		pl2.start("Scanning leaf by leaf");
		for (int leaf = 0; leaf < trie.getNumLeaves(); ++leaf) {
			final DataInputStream leafDis = getLeafStream(leaf);
			if (leafDis == null) {
				++nEmptyLeaves;
				continue;
			}
			prevCrc.trieLeaf = -1;
			for (;;) {
				try {
					crc.load(leafDis);
					assert crc.trieLeaf == leaf : "Leaf ID should be " + leaf + " but is " + crc.trieLeaf;
					++nRec2;
					if (prevCrc.compare(prevCrc, crc) > 0) {
						throw new IllegalStateException("prevCrc=" + prevCrc + " crc=" + crc);
					}
					prevCrc.replace(crc);
				}
				catch (EOFException eofx) {
					break;
				}
			}
			leafDis.close();
			pl2.update();
		}
		pl2.stop("Completed.");
		pl2.done();
		if (nEmptyLeaves > 0) logger.warn(nEmptyLeaves + " empty leaf blocks");
		logger.info(nRec2 + " records");
	}
	
	/*
	 * 2011/06/07 soumen We no longer need to chop up single context training 
	 * file into pieces to be done by separate invocations of this trainer by
	 * ant using fresh JVMs. The fresh JVMs can all access the same context
	 * file and index and process dijoint ranges in them.
	 */
	
	void print(TLongIntHashMap localTrieLeafIdEntIdToCountMap) {
		final LongIntInt lii = new LongIntInt();
		for (TLongIntIterator tle2cx = localTrieLeafIdEntIdToCountMap.iterator(); tle2cx.hasNext(); ) {
			tle2cx.advance();
			lii.write(tle2cx.key());
			final String entName = lii.iv0 >= 0? catalog.entIDToEntName(lii.iv0) : "NA";
			logger.info("L" + lii.iv1 + ", E" + lii.iv0 + "=" + entName + " -> " + tle2cx.value());
		}
	}
	
	/**
	 * Distributed atomic get-increment.
	 * @param leafGenFile shared file used for reserving block
	 * @param blockSize number of leaves to lock (no range check)
	 * @param lowLeaf start here (inclusive)
	 * @param highLeaf end here (exclusive)
	 * @throws IOException
	 */
	synchronized void lockLeafBlock(File leafGenFile, int blockSize, MutableInt lowLeaf, MutableInt highLeaf) throws IOException {
		final RandomAccessFile leafGenRaf = new RandomAccessFile(leafGenFile, "rws");
		leafGenRaf.getChannel().lock();
		if (leafGenRaf.length() == 0) {
			leafGenRaf.writeInt(blockSize);
			lowLeaf.setValue(0);
			highLeaf.setValue(blockSize);
		}
		else {
			leafGenRaf.seek(0);
			lowLeaf.setValue(leafGenRaf.readInt());
			highLeaf.setValue(lowLeaf.intValue() + blockSize);
			leafGenRaf.seek(0);
			leafGenRaf.writeInt(highLeaf.intValue());
		}
//		leafGenLock.release(); Don't do this, it's a bug!
		leafGenRaf.close();
	}
}
