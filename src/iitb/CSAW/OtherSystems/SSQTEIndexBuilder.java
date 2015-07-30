package iitb.CSAW.OtherSystems;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaDocument;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.StringIntBijection;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import cern.colt.list.LongArrayList;

public class SSQTEIndexBuilder {
	/**
	 * @param args
	 * @throws Exception
	 */
	static int px=0;
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		final ACatalog catalog = ACatalog.construct(config);
		final AtomicInteger sharedBatchCounter = new AtomicInteger(0);
		final BarcelonaCorpus corpus = new BarcelonaCorpus(config);
		final TermProcessor termProcessor = DefaultTermProcessor.construct(config);
		final ProgressLogger pl = new ProgressLogger();
		pl.expectedUpdates = corpus.numDocuments();
		pl.displayFreeMemory = true;
		pl.logInterval = 30000;
		corpus.reset();
		pl.start();
		//final StringMap<? extends CharSequence> termMap = DiskBasedIndex.loadStringMap( "/mnt/bag/ganesh/mg4j-index/tokenIndex/token" + DiskBasedIndex.TERMMAP_EXTENSION );
		final StringIntBijection termMap = (StringIntBijection) BinIO.loadObject("/mnt/bag/soumen/barcelona/index/token.bij");
		//final StringMap<? extends CharSequence> entMap = DiskBasedIndex.loadStringMap( "/mnt/bag/ganesh/mg4j-index/entIndex/ent" + DiskBasedIndex.TERMMAP_EXTENSION );
		//final StringIntBijection entMap = (StringIntBijection) BinIO.loadObject("/mnt/bag/soumen/barcelona/index/ent.bij");
		final AtomicIntegerArray entIdCount = new AtomicIntegerArray(catalog.numEnts());
		
		final int numProc = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		//final Index entityIndex = DiskBasedIndex.getInstance("/mnt/b100/d0/ganesh/bag/mg4j-index/entIndex/ent");
		
		if (numProc == 1) {
			final SSQTEIndexWriter iib = new SSQTEIndexWriter(config, catalog, "ent2Type", sharedBatchCounter, termMap, entIdCount, termProcessor.copy());
			final BarcelonaDocument doc = new BarcelonaDocument();
			while (corpus.nextDocument(doc)) {
				iib.indexOneDocument(doc);
				pl.update();
			}
			iib.flushBatch();
		}
		else {
			final WorkerPool workerPool = new WorkerPool(SSQTEIndexBuilder.class, numProc);
			long numDocs = corpus.numDocuments();
			long numDocsPerThread = numDocs/numProc;
			final LongArrayList beginDocIds = new LongArrayList();
			final LongArrayList endDocIds = new LongArrayList();
			long counter = 0, beginDocId = 0;
			for (px = 0; px < numProc; ++px) {
				beginDocIds.add(beginDocId);
				long numDocsRemaining = numDocs - beginDocId; 
				while(counter<Math.min(numDocsPerThread, numDocsRemaining)) counter++;
				long endDocId = beginDocId + counter;
				if(px==numProc-1) endDocId = numDocs;
				endDocIds.add(endDocId);
				beginDocId = endDocId;
				counter = 0;
			}
			System.out.println(beginDocIds);
			System.out.println(endDocIds);
			for (px = 0; px < numProc; ++px) {
				workerPool.add(new IWorker() {
					final SSQTEIndexWriter iib = new SSQTEIndexWriter(config, catalog, "ent2Type", sharedBatchCounter, termMap, entIdCount, termProcessor.copy());
					final BarcelonaDocument doc = new BarcelonaDocument();
					long wNumDone = 0;
					
					@Override
					public Exception call() throws Exception {
						try {
							for(long docId=beginDocIds.get(px-1); docId<endDocIds.get(px-1); docId++){
							//while(wikiEntCorpus.nextDocument(entDoc, bal)) {
								corpus.getDocument(docId, doc);
								//System.out.println("docId=("+px+")"+tokenDoc.docidAsInt()+";"+entDoc.docidAsInt());
								iib.indexOneDocument(doc);
								pl.update();
								++wNumDone;
							}
							iib.flushBatch();
							return null;
						}
						catch (Exception any) {
							return any;
						}
					}

					@Override
					public long numDone() {
						return wNumDone;
					}
				});
			}
			workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		}
		corpus.close();
	}
}
