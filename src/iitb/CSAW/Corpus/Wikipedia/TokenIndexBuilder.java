package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.Util;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.tool.ScanBatch;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class TokenIndexBuilder {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log
	 * @throws Throwable 
	 */
	public static void main(String[] args) throws Throwable  {
		final Config config = new Config(args[0], args[1]);
		TokenIndexBuilder wib = new TokenIndexBuilder(config);
		wib.run();
		wib.close();
	}

	final Config config;
	final AStripeManager stripeManager;
	final BarcelonaCorpus wrar;
	final File indexDir;
	final AtomicInteger sharedBatchNumber = new AtomicInteger();
	final TermProcessor termProcessor;
	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);
	
	TokenIndexBuilder(Config config) throws EnvironmentLockedException, IOException, DatabaseException, InstantiationException, IllegalAccessException, ClassNotFoundException, IllegalArgumentException, SecurityException, InvocationTargetException, NoSuchMethodException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		this.wrar = new BarcelonaCorpus(config);
		this.indexDir = stripeManager.myTokenIndexRunDir();
		this.termProcessor = DefaultTermProcessor.construct(config);
	}
	
	void close() throws IOException, DatabaseException {
		wrar.close();
	}
	
	class Worker implements IWorker {
		long wNumDone = 0;
		
		@Override
		public Exception call() throws Exception {
			try {
				BarcelonaDocument mdoc = new BarcelonaDocument();
				ScanBatch bscan = null;
				for (int numDocsDone = 0; wrar.nextDocument(mdoc); ++numDocsDone) {
					if (bscan == null) {
						final int localBatchNumber = sharedBatchNumber.getAndIncrement();
						bscan = new ScanBatch(indexDir, Field.token.toString(), localBatchNumber, termProcessor.copy());
					}
					final boolean keepGoing = bscan.scanOneDocument(mdoc);
					if (!keepGoing) {
						bscan = null;
					}
					pl.update();
					++wNumDone;
				}
				if (bscan != null) {
					bscan.finish();
				}
			}
			catch (AssertionError ae) {
				ae.printStackTrace();
				logger.fatal(ae);
				System.exit(-1);
			}
			catch (OutOfMemoryError oom) {
				oom.printStackTrace();
				logger.fatal(oom);
				System.exit(-1);
			}
			catch (Exception anyex) {
				logger.fatal(anyex);
				anyex.printStackTrace();
			}
			return null;
		}

		@Override
		public long numDone() {
			return wNumDone;
		}
	}
	
	void run() throws Throwable {
		final int numThreads = config.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, numThreads);
		wrar.reset();
		sharedBatchNumber.set(0);
		pl.displayFreeMemory = true;
		pl.expectedUpdates = wrar.numDocuments();
		pl.start("Started indexing.");
		if (numThreads == 1) {
			new Worker().call();
		}
		else {
			for (int tx = 0; tx < numThreads; ++tx) {
				final Worker worker = new Worker();
				workerPool.add(worker);
			}
			workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		}
		pl.stop("Finished index run generation.");
		pl.done();
		
		/*
		 * 2010/12/14 soumen -- Merge should be done by a separate class.
		*/
	}
}
