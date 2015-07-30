package iitb.CSAW.Index.SIP1;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Index.PropertyKeys;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.Sort.ExternalMergeSort;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;

public class Sip1IndexMerger {
	/**
	 * @param args [0]=prop [1]=log [2]=fieldName
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		final String fieldName = args[2];
		Sip1IndexMerger iim = new Sip1IndexMerger(config, fieldName);
		iim.merge();
		iim.compress();
//		iim.scanCompressed();
	}

	static final Logger logger = Logger.getLogger(Sip1IndexMerger.class);
	final Config config;
	final AStripeManager stripeManager;
	final String fieldName;
	final File tmpDir, largeFile, smallPostingsFile, smallOffsetsFile, smallNdocsFile;
	
	protected Sip1IndexMerger(Config config, String fieldName) throws Exception {
		this.config = config;
		this.fieldName = fieldName;
		stripeManager = AStripeManager.construct(config);
		final URI sipIndexHostUri = stripeManager.sipIndexHostDir(stripeManager.myHostStripe());
		final File indexDir = new File(sipIndexHostUri.getPath());
		this.largeFile = new File(indexDir, fieldName + PropertyKeys.sipUncompressedPostingsExtension);
		this.smallPostingsFile = new File(indexDir, fieldName + PropertyKeys.sipCompactPostingExtension);
		this.smallOffsetsFile = new File(indexDir, fieldName + PropertyKeys.sipCompactOffsetExtension);
		this.smallNdocsFile = new File(indexDir, fieldName + PropertyKeys.sipCompactNumDocExtension);
		this.tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
	}

	void compress() throws IOException {
		final Int2LongOpenHashMap catToSeek = new Int2LongOpenHashMap();
		catToSeek.defaultReturnValue(-1);
		final Int2IntOpenHashMap catToNDoc = new Int2IntOpenHashMap();
		catToNDoc.defaultReturnValue(-1);
		final DataInputStream largeDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(largeFile)));
		final OutputBitStream postingObs = new OutputBitStream(smallPostingsFile);
		final Sip1Document idp = new Sip1Document();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.start("Compressing interim postings.");
		for (int prevCat = -1, prevDoc = -1, nDoc = 0; ; ) {
			try {
				idp.load(largeDis);
				final int cat = idp.catId();
				final int doc = idp.docId();
				assert doc > prevDoc || cat > prevCat;
				if (cat != prevCat) {
					final long bitSeek = postingObs.writtenBits();
					catToSeek.put(cat, bitSeek);
					catToNDoc.put(prevCat, nDoc);
					nDoc = 0;
				}
				idp.storeCompressed(postingObs);
				++nDoc;
				pl.update();
				prevCat = cat;
				prevDoc = doc;
			}
			catch (EOFException eofx) {
				if (prevCat != -1) {
					catToNDoc.put(prevCat, nDoc);
				}
				break;
			}
		}
		pl.stop("Done.");
		postingObs.close();
		largeDis.close();
		BinIO.storeObject(catToSeek, smallOffsetsFile);
		BinIO.storeObject(catToNDoc, smallNdocsFile);
		for (int nDocs : catToNDoc.values()) {
			MonitorFactory.add("nDocs", "", nDocs);
		}
		logger.info(MonitorFactory.getMonitor("nDocs", ""));
		pl.done();
	}
	
	void scanCompressed() throws IOException, ClassNotFoundException {
		final Int2LongOpenHashMap catToSeek = (Int2LongOpenHashMap) BinIO.loadObject(smallOffsetsFile);
		final Int2IntOpenHashMap catToNDoc = (Int2IntOpenHashMap) BinIO.loadObject(smallNdocsFile);
		final InputBitStream postingIbs = new InputBitStream(smallPostingsFile);
		final ProgressLogger pl = new ProgressLogger(logger);
		final Sip1Document idp = new Sip1Document();
		pl.expectedUpdates = catToSeek.size();
		pl.start("Scanning compressed postings.");
		int junk = 0;
		for (int catId : catToSeek.keySet()) {
			final long bitSeek = catToSeek.get(catId);
			final int nDocs = catToNDoc.get(catId);
			postingIbs.position(bitSeek);
			for (int dx = 0; dx < nDocs; ++dx) {
				idp.loadCompressed(postingIbs, catId);
				junk += idp.docId();
			}
			pl.update();
		}
		pl.stop("Done, junk=" + junk);
		postingIbs.close();
		pl.done();
	}
	
	void merge() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException, URISyntaxException {
		ArrayList<File> runs = new ArrayList<File>();
		logger.info("Finding runs in " + stripeManager.mySipIndexRunDir());
		for (File mayBeRunFile : stripeManager.mySipIndexRunDir().listFiles()) {
			final String mayBeName = mayBeRunFile.getName();
			if (mayBeName.startsWith(fieldName) && mayBeName.endsWith(PropertyKeys.sipUncompressedPostingsExtension)) {
				runs.add(mayBeRunFile);
			}
		}
		logger.info("Merging " + runs);
		final int nThreads = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		Comparator<Sip1Document> comparator = new Comparator<Sip1Document>() {
			@Override
			public int compare(Sip1Document o1, Sip1Document o2) {
				final int c1 = o1.catId() - o2.catId();
				if (c1 != 0) return c1;
				final int c2 = o1.docId() - o2.docId();
				return c2;
			}
		};
		ExternalMergeSort<Sip1Document> ems = new ExternalMergeSort<Sip1Document>(Sip1Document.class, comparator, false, tmpDir);
		final URI outputUri = stripeManager.sipIndexHostDir(stripeManager.myHostStripe());
		final File outputDir = new File(outputUri.getPath());
		final File outputFile = new File(outputDir, fieldName + PropertyKeys.sipUncompressedPostingsExtension);
		if (nThreads == 1) {
			ems.mergeSequential(runs, outputFile);
		}
		else {
			ems.mergeParallel(runs, outputFile, nThreads);
		}
		for (File run : runs) {
			if (!run.delete()) {
				logger.warn("Could not delete " + run);
			}
		}
	}
}
