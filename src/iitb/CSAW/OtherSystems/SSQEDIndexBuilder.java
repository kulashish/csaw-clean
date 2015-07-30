package iitb.CSAW.OtherSystems;

import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Catalog.YAGO.YagoCatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaDocument;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.Util;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.tool.SSQScanBatch;
import it.unimi.dsi.mg4j.tool.ScanBatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class SSQEDIndexBuilder {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception  {
		final Config config = new Config(args[0], args[1]);
		SSQEDIndexBuilder wib = new SSQEDIndexBuilder(config);
		final int nThreads = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		wib.run(nThreads);
		wib.close();
	}

	final Config config;
	final BarcelonaCorpus corpus;
	final File indexDir;
	final AtomicInteger sharedBatchNumber = new AtomicInteger();
	final Field field = Field.ent;
	final TermProcessor termProcessor;
	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);
	TIntIntHashMap atypeCountMap = new TIntIntHashMap(); 
	ACatalog catalog;
	
	SSQEDIndexBuilder(Config config) throws EnvironmentLockedException, IOException, DatabaseException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.config = config;
		this.corpus = new BarcelonaCorpus(config);
		this.indexDir = new File(config.getString(iitb.CSAW.OtherSystems.PropertyKeys.indexDirKey));
		this.termProcessor = DefaultTermProcessor.construct(config);
		catalog = YagoCatalog.getInstance(config); 
		
		/**Loading queries and registering all the unique entityIDs and their counts in an RBTreemap*/
		BufferedReader br = new BufferedReader(new FileReader("/mnt/b100/d0/devshree/queries/atypes.sorted"));
		String line = null;
		while((line=br.readLine())!=null){
			line.trim();
			atypeCountMap.adjustOrPutValue(catalog.catNameToCatID(line),1,1);
		}
		/*HashMap<String, RootQuery> queries  = new HashMap<String, RootQuery>();
		File qDir = new File(config.getString(iitb.CSAW.EntityRank.Wikipedia.PropertyKeys.batchQueryDirKey));
		final int slop = config.getInt(PropertyKeys.windowKey);
		QueryWithAnswers.loadQueries2(qDir, logger, slop, queries);
		for (RootQuery query : queries.values()) {
			for (ContextQuery contextQuery : query.contexts) {
				for (MatcherQuery matcherQuery : contextQuery.matchers) {
					if (matcherQuery instanceof TypeBindingQuery) {
						final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
						int typeId = catalog.catNameToCatID(typeBindingQuery.typeName);
						System.out.println(typeBindingQuery.typeName+","+typeId);
						
						atypeCountMap.adjustOrPutValue(typeId,1,1);
					}
				}
			}
		}*/
		
	}
	
	void close() throws IOException, DatabaseException {
		corpus.close();
	}
	
	class Worker implements Runnable {
		@Override
		public void run() {
			try {
				final BarcelonaDocument mdoc = new BarcelonaDocument();
				ScanBatch bscan = null;
				for (int numDocsDone = 0; corpus.nextDocument(mdoc); ++numDocsDone) {
					if (bscan == null) {
						final int localBatchNumber = sharedBatchNumber.getAndIncrement();
						bscan = new SSQScanBatch(indexDir, field.toString(), localBatchNumber, termProcessor.copy(), catalog, atypeCountMap);
					}
					final boolean keepGoing = bscan.scanOneDocument(mdoc);
					if (!keepGoing) {
						bscan = null;
					}
					pl.update();
				}
				if (bscan != null) {
					bscan.finish();
				}
			}
			catch (Exception anyex) {
				logger.fatal(anyex);
				anyex.printStackTrace();
			}
		}
	}
	
	void run(int numThreads) throws Exception {
		corpus.reset();
		sharedBatchNumber.set(0);
		pl.displayFreeMemory = true;
		pl.expectedUpdates = corpus.numDocuments();
		pl.start("indexing");
		
		if (numThreads == 1) {
			new Worker().run();
		}
		else {
			ArrayList<Thread> workers = new ArrayList<Thread>();
			for (int tx = 0; tx < numThreads; ++tx) {
				Thread worker = new Thread(new Worker(), "Worker" + tx);
				workers.add(worker);
				worker.start();
			}
			for (Thread worker : workers) {
				worker.join();
			}
		}
		
		pl.done();
		
		// TODO check if everyone did ok before trying to merge
		
		// merge the runs -- this part is not threaded yet
		/*final boolean interleaved = false, skips = true;
		for (int atypeId : atypeCountMap.keys()) {
			String atypeName = field.toString()+":"+catalog.catIDToCatName(atypeId);
			ArrayList<String> inputNames = new ArrayList<String>();
			for (int bx = 0; bx < sharedBatchNumber.intValue(); ++bx) {
				inputNames.add(SSQScanBatch.batchBasename(bx, atypeName, indexDir));
			}
			System.out.println(inputNames);
			final String[] inputBasename = inputNames.toArray(new String[]{});
			
			new Merge(new File(indexDir, atypeName).toString(), inputBasename, false, Combine.DEFAULT_BUFFER_SIZE, CompressionFlags.DEFAULT_STANDARD_INDEX, interleaved, skips, BitStreamIndex.DEFAULT_QUANTUM, BitStreamIndex.DEFAULT_HEIGHT, SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL).run();
			//new Merge(new File(indexDir, field.toString()).toString(), inputBasename, false, Combine.DEFAULT_BUFFER_SIZE, CompressionFlags.DEFAULT_STANDARD_INDEX, interleaved, skips, BitStreamIndex.DEFAULT_QUANTUM, BitStreamIndex.DEFAULT_HEIGHT, SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE, ProgressLogger.DEFAULT_LOG_INTERVAL).run();
			Class<? extends StringMap<? extends CharSequence>> termMapClass = ImmutableExternalPrefixMap.class;
			logger.info( "Creating term maps (class: " + termMapClass.getSimpleName() + ")..." );
			final String baseNameField = new File(indexDir, atypeName).toString();
			BinIO.storeObject( StringMaps.synchronize( termMapClass.getConstructor( Iterable.class ).newInstance( new FileLinesCollection( baseNameField + DiskBasedIndex.TERMS_EXTENSION, "UTF-8" ) ) ), baseNameField + DiskBasedIndex.TERMMAP_EXTENSION );
		}*/
		logger.info( "Indexing completed." );
	}
}
