package iitb.CSAW.OtherSystems;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Index.SIP1.Sip1Document;
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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;

public class SSQIndexMerger {
	/**
	 * @param args [0]=prop [1]=log [2]=fieldName
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		final String fieldName = args[2];
		SSQIndexMerger iim = new SSQIndexMerger(config, fieldName);
		iim.merge();
		iim.compress();
//		iim.scanCompressed();
	}

	static final Logger logger = Logger.getLogger(SSQIndexMerger.class);
	final Config config;
	final AStripeManager stripeManager;
	final String fieldName;
	final File tmpDir, indexDir, largeFile, smallPostingsFile, smallOffsetsFile, smallNumEntsFile;
	
	protected SSQIndexMerger(Config config, String fieldName) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		this.fieldName = fieldName;
		this.indexDir = new File(config.getString(iitb.CSAW.OtherSystems.PropertyKeys.indexDirKey));
		this.largeFile = new File(indexDir, fieldName + iitb.CSAW.OtherSystems.PropertyKeys.ssqteInterimExtension);
		this.smallPostingsFile = new File(indexDir, fieldName + iitb.CSAW.OtherSystems.PropertyKeys.ssqCompactPostingExtension);
		this.smallOffsetsFile = new File(indexDir, fieldName + iitb.CSAW.OtherSystems.PropertyKeys.ssqCompactOffsetExtension);
		this.smallNumEntsFile = new File(indexDir, fieldName + iitb.CSAW.OtherSystems.PropertyKeys.ssqCompactNumEntsExtenstion);
		this.tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe()); 
	}

	void compress() throws IOException {
		final Int2LongOpenHashMap tokIdToSeek = new Int2LongOpenHashMap();
		tokIdToSeek.defaultReturnValue(-1);
		final Int2IntOpenHashMap tokIdToNumEnts = new Int2IntOpenHashMap();
		tokIdToNumEnts.defaultReturnValue(-1);
		final DataInputStream largeDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(largeFile)));
		final OutputBitStream postingObs = new OutputBitStream(smallPostingsFile);
		final SSQEntityBlock idp = new SSQEntityBlock();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.start("Compressing interim postings.");
		ArrayList<SSQEntityBlock> pendingBlocksForAnEntity = new ArrayList<SSQEntityBlock>();
		int numEnts = 0, prevEntId = -1, prevTokId = -1, npost = 0;
		for (; ; ) {
			try {
				idp.load(largeDis);
				final int tokId = idp.tokId();
				final int entId = idp.entId();
				assert entId >= prevEntId || tokId > prevTokId;
				
				if((entId != prevEntId && prevEntId != -1 && tokId == prevTokId) ||
						(tokId != prevTokId && prevTokId!=-1)){
					++numEnts;	
					SSQEntityBlock idp1 = pendingBlocksForAnEntity.get(0);
					idp1.storeCompressed(postingObs, npost);
					for(int i=1; i<pendingBlocksForAnEntity.size(); i++){
						pendingBlocksForAnEntity.get(i).storeCompressedPostings(postingObs);
					}
					pendingBlocksForAnEntity.clear();
					npost=0;
				}
				if (tokId != prevTokId) {
					final long bitSeek = postingObs.writtenBits();
					tokIdToSeek.put(tokId, bitSeek);
					if (prevTokId != -1){
						tokIdToNumEnts.put(prevTokId, numEnts);
						numEnts = 0;
					}
				}
				pl.update();
				prevTokId = tokId;
				prevEntId = entId;
				npost += idp.getPostSize();
				pendingBlocksForAnEntity.add(idp.clone());
				
			}
			catch (EOFException eofx) {
				if (prevTokId != -1) {
					++numEnts;	
					SSQEntityBlock idp1 = pendingBlocksForAnEntity.get(0);
					idp1.storeCompressed(postingObs, npost);
					for(int i=1; i<pendingBlocksForAnEntity.size(); i++){
						pendingBlocksForAnEntity.get(i).storeCompressedPostings(postingObs);
					}
					pendingBlocksForAnEntity.clear();
					npost=0;
					tokIdToNumEnts.put(prevTokId, numEnts);
					numEnts = 0;
				}
				break;
			}
		}
		pl.stop("Done.");
		postingObs.close();
		largeDis.close();
		BinIO.storeObject(tokIdToSeek, smallOffsetsFile);
		BinIO.storeObject(tokIdToNumEnts, smallNumEntsFile);
		for (int nEnts : tokIdToNumEnts.values()) {
			MonitorFactory.add("numEnts", "", nEnts);
		}
		logger.info(MonitorFactory.getMonitor("numEnts", ""));
		pl.done();
	}
	
	void scanCompressed() throws IOException, ClassNotFoundException {
		final Int2LongOpenHashMap catToSeek = (Int2LongOpenHashMap) BinIO.loadObject(smallOffsetsFile);
		final Int2IntOpenHashMap catToNDoc = (Int2IntOpenHashMap) BinIO.loadObject(smallNumEntsFile);
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
	
	void merge() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		ArrayList<File> runs = new ArrayList<File>();
		for (File mayBeRunFile : indexDir.listFiles()) {
			final String mayBeName = mayBeRunFile.getName();
			if (mayBeName.startsWith(fieldName) && mayBeName.endsWith(iitb.CSAW.OtherSystems.PropertyKeys.ssqteInterimExtension)) {
				runs.add(mayBeRunFile);
			}
		}
		System.err.println("runs="+runs);
		
		final int nThreads = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		Comparator<SSQEntityBlock> comparator = new Comparator<SSQEntityBlock>() {
			@Override
			public int compare(SSQEntityBlock o1, SSQEntityBlock o2) {
				final int c1 = o1.tokId() - o2.tokId();
				if (c1 != 0) return c1;
				final int c2 = o1.entId() - o2.entId();
				return c2;
			}
		};
		ExternalMergeSort<SSQEntityBlock> ems = 
			new ExternalMergeSort<SSQEntityBlock>(SSQEntityBlock.class, comparator, false, tmpDir);
		
		final File outputFile = new File(indexDir, fieldName + iitb.CSAW.OtherSystems.PropertyKeys.ssqteInterimExtension);
		if (nThreads == 1) {
			ems.mergeSequential(runs, outputFile);
		}
		else {
			ems.mergeParallel(runs, outputFile, nThreads);
		}
		/*
		for (File run : runs) {
			if (!run.delete()) {
				logger.warn("Could not delete " + run);
			}
		}
		*/
	}
}
