package iitb.CSAW.Utils;

import it.unimi.dsi.lang.MutableString;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class WorkerPool {
	final Logger logger;
	final ArrayList<Future<Exception>> futures = new ArrayList<Future<Exception>>();
	final ArrayList<IWorker> workers = new ArrayList<IWorker>();
	final ExecutorService executorService;
	
	public WorkerPool(Object obj, int nThreads) {
		logger = Logger.getLogger(obj.getClass());
		executorService = Executors.newFixedThreadPool(nThreads);
	}
	
	public void add(IWorker wt) {
		workers.add(wt);
		futures.add(executorService.submit(wt));
	}
	
	/**
	 * Call after {@link #add(IWorker)}ing all workers
	 * @param intervalMillis poll every so often for completion
	 * @param stub run this every poll iteration if not null
	 * @throws Exception
	 */
	public void pollToCompletion(long intervalMillis, Runnable stub) throws Exception {
		final long beginTime = System.currentTimeMillis();
		executorService.shutdown();
		final MutableString rates = new MutableString();
		for(long prevDone = 0; !executorService.awaitTermination(intervalMillis, TimeUnit.MILLISECONDS); ) {
			if (stub != null) {
				stub.run();
			}
			long allNumDone = 0;
			rates.length(0);
			final long nowTime = System.currentTimeMillis();
			final double elapsedTime = nowTime - beginTime;
			for (IWorker wt : workers) {
				final long aNumDone = wt.numDone(); 
				allNumDone += aNumDone;
				rates.append(String.format("%5g ", 1000d * aNumDone / elapsedTime));
			}
			final double cumulativeRate = 1000d * allNumDone / (nowTime - beginTime);
			final double windowRate = 1000d * (allNumDone - prevDone) / intervalMillis;
			logger.info("WATCHDOG time " + (nowTime - beginTime)/1000 + " cumulative " + cumulativeRate + " window " + windowRate);
			logger.info("WATCHDOG rates " + rates);
			int nAlive = futures.size();
			for (Future<Exception> ferr : futures) {
				if (ferr.isDone()) {
					--nAlive;
					if (ferr.get() != null) {
						logger.error("Terminated with error", ferr.get());
						ferr.get().printStackTrace();
					}
				}
			}
			logger.info("Threads alive " + nAlive);
			if (nAlive == 0) {
				break;
			}
			prevDone = allNumDone;
		}
		while (!executorService.awaitTermination(intervalMillis, TimeUnit.MILLISECONDS)) {
			logger.info("Waiting for termination " + System.currentTimeMillis() + " [" + new Date() + "]");
		}
		logger.info("Completed.");
	}
}
