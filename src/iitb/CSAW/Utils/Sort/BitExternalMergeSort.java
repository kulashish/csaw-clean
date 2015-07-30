package iitb.CSAW.Utils.Sort;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;

public class BitExternalMergeSort<T extends IBitRecord<T>> {
	private static final int MAX_FAN_IN = 200;
	
	/**
	 * These may need to be tuned depending on the CPU work involved in
	 * {@link IBitRecord#load(InputBitStream)},
	 * {@link IBitRecord#store(OutputBitStream)}, and
	 * comparing two {@link IBitRecord}s.
	 */
	private static final int FAST_BUFFER_SIZE = 4*(1<<20), BLOCKING_QUEUE_SIZE = 4*(1<<10);

	final Logger logger = Logger.getLogger(getClass());
	final File tmpDir;
	protected Class<? extends T> type;
	protected Comparator<T> comparator;

	public BitExternalMergeSort(Class<? extends T> type, File tmpDir) throws InstantiationException, IllegalAccessException {
		this.tmpDir = tmpDir;
		this.type = type;
		this.comparator = type.newInstance().getComparator();
	}
	
	/**
	 * Single threaded.
	 * @param inputFiles
	 * @param outputFile
	 */
	public void mergeUsingHeap(Collection<File> inputFiles, File outputFile) throws IOException, InstantiationException, IllegalAccessException {
		final Reference2ReferenceOpenHashMap<T, InputBitStream> headToRest = new Reference2ReferenceOpenHashMap<T, InputBitStream>();
		final ObjectHeapPriorityQueue<T> mergeHeap = new ObjectHeapPriorityQueue<T>(comparator);
		for (File inRun : inputFiles) {
			InputBitStream ibs = getBufferedInputBitStream(inRun);
			T rec = type.newInstance();
			try {
				rec.load(ibs);
				headToRest.put(rec, ibs);
				mergeHeap.enqueue(rec);
			}
			catch (EOFException eofx) {
				ibs.close(); // and skip this stream
			}
		}
		final OutputBitStream obs = getBufferedOutputBitStream(outputFile);
		while (!mergeHeap.isEmpty()) {
			final T first = mergeHeap.dequeue();
			first.store(obs);
			final InputBitStream rest = headToRest.get(first);
			try {
				first.load(rest);
				mergeHeap.enqueue(first);
			}
			catch (EOFException eofx) {
				rest.close();
				headToRest.remove(first);
			}
		}
		if (!headToRest.isEmpty()) {
			final String message = "Merge heap is empty but headToRest is not!"; 
			logger.fatal(message);
			throw new IllegalStateException(message);
		}
		obs.close();
	}
	
	public void mergeUsingHeapLimitedFanIn(Collection<File> iFiles, File oFile) throws IOException, InstantiationException, IllegalAccessException {
		final LinkedList<File> iManyFiles = new LinkedList<File>(iFiles);
		final HashSet<File> filesToDelete = new HashSet<File>();
		while (!iManyFiles.isEmpty()) {
			if (iManyFiles.size() < MAX_FAN_IN) {
				mergeUsingHeap(iManyFiles, oFile);
				break;
			}
			final ArrayList<File> iFewFiles = new ArrayList<File>();
			while (iFewFiles.size() < MAX_FAN_IN) {
				iFewFiles.add(iManyFiles.remove());
			}
			final File f2d = File.createTempFile(type.getCanonicalName() + "_", ".dat", tmpDir);
			filesToDelete.add(f2d);
			f2d.deleteOnExit();
			iManyFiles.add(f2d);
			mergeUsingHeap(iFewFiles, f2d);
			for (File f2do : iFewFiles) {
				if (filesToDelete.contains(f2do)) { // eager delete
					filesToDelete.remove(f2do);
					if (!f2do.delete()) {
						logger.error("Could not delete " + f2d);
					}
				}
			}
		}
		if (!filesToDelete.isEmpty()) {
			logger.warn("Files to delete: " + filesToDelete);
		}
	}
	

	/**
	 * Multithreaded with producer-consumer buffers.
	 * Typically makes things only slower and/or consumes lots of RAM.
	 * @param inputFiles
	 * @param outputFile
	 * @param nThreads
	 */
	@Deprecated
	public void mergeUsingHeap(Collection<File> inputFiles, File outputFile, int nThreads) throws RuntimeException, IOException, InterruptedException, ExecutionException {
		final ExecutorService es = new ThreadPoolExecutor(inputFiles.size()+1, inputFiles.size()+1, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
		final OutputBitStream obs = getBufferedOutputBitStream(outputFile);
		final ArrayList<BufferedIBitRecordReader> inDiss = new ArrayList<BufferedIBitRecordReader>();
		final ArrayList<Future<Throwable>> futures = new ArrayList<Future<Throwable>>();
		for (File inRun : inputFiles) {
			final BufferedIBitRecordReader bibrr = new BufferedIBitRecordReader(inRun);
			inDiss.add(bibrr);
			futures.add(es.submit(bibrr));
		}
		final Reference2ReferenceOpenHashMap<T, BufferedIBitRecordReader> headToRest = new Reference2ReferenceOpenHashMap<T, BufferedIBitRecordReader>();
		final ObjectHeapPriorityQueue<T> mergeHeap = new ObjectHeapPriorityQueue<T>(comparator);
		for (BufferedIBitRecordReader inDis : inDiss) {
			for (;;) {
				final T rec = inDis.poll(10L, TimeUnit.SECONDS);
				if (rec != null) {
					if (!rec.isNull()) {
						mergeHeap.enqueue(rec);
						headToRest.put(rec, inDis);
					}
					break;
				}
				Thread.yield();
			}
		}
		while (!mergeHeap.isEmpty()) {
			MonitorFactory.add("HeapSize", null, mergeHeap.size());
			final T first = mergeHeap.dequeue();
			first.store(obs);
			final BufferedIBitRecordReader bibrr = headToRest.remove(first);
			MonitorFactory.add("QueueSize", null, bibrr.size());
			final T next = bibrr.take();
			if (!next.isNull()) {
				mergeHeap.enqueue(next);
				headToRest.put(next, bibrr);
			}
		}
		for (BufferedIBitRecordReader inDis : inDiss) {
			inDis.close();
		}
		obs.close();
		for (Future<Throwable> future : futures) {
			final Throwable throwable = future.get();
			if (throwable != null) {
				throw new RuntimeException(throwable); // relay
			}
		}
		es.shutdown();
		while (!es.awaitTermination(10L, TimeUnit.SECONDS)) {
			logger.warn("Merge thread should not have to wait.");
		}
		logger.debug("Average heap size=" + MonitorFactory.getMonitor("HeapSize", null).getAvg() + " average buffer fill=" + MonitorFactory.getMonitor("QueueSize", null).getAvg()/BLOCKING_QUEUE_SIZE);
	}
	
	@SuppressWarnings("serial")
	class BufferedIBitRecordReader extends LinkedBlockingQueue<T> implements Callable<Throwable> {
		final File file;
		final InputBitStream ibs;
		
		BufferedIBitRecordReader(File file) throws FileNotFoundException {
			super(BLOCKING_QUEUE_SIZE);
			this.file = file;
			ibs = getBufferedInputBitStream(file);
		}
		
		void close() throws IOException {
			ibs.close();
		}

		@Override
		public Throwable call() throws Exception {
			int nRec = 0;
			for (;;) {
				try {
					final T rec = type.newInstance();
					rec.load(ibs);
					++nRec;
					put(rec);
				}
				catch (EOFException eofx) {
					final T eof = type.newInstance();
					eof.setNull();
					put(eof);
					break;
				}
				catch (Throwable tx) {
					logger.error("Error reading " + file + " at record " + nRec, tx);
					return tx;
				}
			}
			logger.trace(Integer.toHexString(System.identityHashCode(this)) + " done after " + nRec + " records");
			return null;
		}
	}

	protected static InputBitStream getBufferedInputBitStream(File inRun) throws FileNotFoundException {
		return new InputBitStream(new FastBufferedInputStream(new FileInputStream(inRun), FAST_BUFFER_SIZE));
	}

	protected static OutputBitStream getBufferedOutputBitStream(File outputFile) throws FileNotFoundException {
		return new OutputBitStream(new FastBufferedOutputStream(new FileOutputStream(outputFile), FAST_BUFFER_SIZE));
	}
}
