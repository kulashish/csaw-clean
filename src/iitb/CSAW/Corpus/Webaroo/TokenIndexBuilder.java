package iitb.CSAW.Corpus.Webaroo;

import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.tool.ScanBatch;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.RuntimeErrorException;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

public class TokenIndexBuilder {
	/**
	 * Turn RAR format corpus into MG4J index local runs.
	 * @param args [0]=/path/to/properties [1]=/path/to/log/file
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		TokenIndexBuilder ib = new TokenIndexBuilder(config);
		ib.build();
		ib.close();
	}

	static final long LOG_INTERVAL = ProgressLogger.ONE_MINUTE;
	final TermProcessor termProcessor;
	static final String FIELD_NAME = ACorpus.Field.token.toString();
	final Config config;
	final Logger logger;
	final ProgressLogger pl;
	final WebarooStripeManager stripeManager;
	final File rarBaseDir, indexBaseDir;
	final WebarooCorpus wCorpus1;
	final AtomicInteger batch = new AtomicInteger(0);

	TokenIndexBuilder(Config config) throws Exception {
		this.config = config;
		this.stripeManager = new WebarooStripeManager(config);
//		config.save(System.out);
		termProcessor = DefaultTermProcessor.construct(config);
		this.logger = Logger.getLogger(getClass());
		final URI corpus1Uri = stripeManager.corpusDir(stripeManager.myDiskStripe());
		assert corpus1Uri.getHost().equals(stripeManager.myHostName());
		this.rarBaseDir = new File(corpus1Uri.getPath());
		this.indexBaseDir = stripeManager.myTokenIndexRunDir();
		wCorpus1 = new WebarooCorpus(config, rarBaseDir, false, false, true);
		pl = new ProgressLogger(logger);
		pl.logInterval = LOG_INTERVAL;
		pl.displayFreeMemory = true;
//		pl.expectedUpdates = wCorpus1.numDocuments() / stripeManager.buddyHostStripes(stripeManager.myDiskStripe()).size();
	}
	
	void close() throws InterruptedException, IOException, DatabaseException {
		logger.info(getClass() + " close " + new Date());
		wCorpus1.close();
	}
	
	void build() throws Exception {
		if (!WebarooCorpus.isReady(rarBaseDir)) {
			logger.error(rarBaseDir + " is not ready for reading");
			return;
		}
		logger.info(getClass() + " build " + new Date());
		pl.start();
		/*
		 * rarBaseDir is a Corpus1, so we need a single scanner accessed
		 * concurrently by a fixed number of threads.
		 */
		final int numProc = config.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, numProc);
		for (int tx = 0; tx < numProc; ++tx) {
			workerPool.add(new Worker());
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		pl.stop();
		pl.done();
		// touch some file signaling error-free termination
	}
	
	class Worker implements IWorker {
		long wNumDone = 0;
		
		@Override
		public Exception call() throws Exception {
			try {
				WebarooDocument wdoc = (WebarooDocument) wCorpus1.allocateReusableDocument();
				ByteArrayList workingSpace = new ByteArrayList();
				ScanBatch bscan = null;
				for (;;) {
					try {
						if (!wCorpus1.nextDocument(wdoc, workingSpace)) {
							break;
						}
						if (stripeManager.isMyJob(wdoc.docidAsLong())) {
							continue;
						}
						if (bscan == null) {
							bscan = new ScanBatch(indexBaseDir, FIELD_NAME, batch.getAndIncrement(), termProcessor.copy());
						}
						final boolean keepGoing = bscan.scanOneDocument(wdoc);
						if (!keepGoing) {
							bscan = null;
						}
						pl.update();
						++wNumDone;
					}
					catch (Exception anyx) {
						logger.error("Trouble after " + wNumDone + " documents: ", anyx);
					}
				}
				logger.info(Thread.currentThread() + " indexed " + wNumDone + " docs");
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
			catch (Exception anyx) {
				logger.error(anyx);
				return anyx;
			}
			catch (Error anye) {
				logger.error(anye);
				return new RuntimeErrorException(anye);
			}
			return null;
		}

		@Override
		public long numDone() {
			return wNumDone;
		}
	}
}
