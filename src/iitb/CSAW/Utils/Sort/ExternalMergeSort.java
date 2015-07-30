package iitb.CSAW.Utils.Sort;

import iitb.CSAW.Utils.MemoryStatus;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

public class ExternalMergeSort<T extends IRecord> {
	static final String suffix = ".dat";
	static final int FAST_BUFFER_SIZE = 8*(1<<20);
	static final double minFreeFraction = 0.3;
	final Logger logger = Logger.getLogger(getClass());
	final File tempDir;
	final Class<T> type;
	final Comparator<T> comparator;
	final boolean doUnique;
	final MemoryStatus memStat = new MemoryStatus();
	final MutableLong usedMem = new MutableLong(), freeMem = new MutableLong(), availMem = new MutableLong(), totalMem = new MutableLong(), maxMem = new MutableLong();
	private transient final IntArrayList runSorter = new IntArrayList();
	private transient int runSize = -1;
	
	public ExternalMergeSort(Class<T> type, Comparator<T> comparator, boolean doUnique, File tempDir) throws InstantiationException, IllegalAccessException {
		this.tempDir = tempDir;
		this.doUnique = doUnique;
		this.type = type;
		this.comparator = comparator;
	}
	
	public void setRunSize(int newRunSize) {
		runSize = newRunSize;
	}
	
	public void run(File inf, File outf) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		final Collection<File> runFiles = writeSortedRuns(inf);
		mergeSequential(runFiles, outf);
		deleteTempFiles(runFiles);
	}
	
	public void runParallel(File inf, File outf, int nThreads) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InterruptedException, ExecutionException {
		final Collection<File> runFiles = writeSortedRuns(inf);
		mergeParallel(runFiles, outf, nThreads);
		deleteTempFiles(runFiles);
	}
	
	public void mergeFanIn(Collection<File> runFiles, File outf) throws IOException, InstantiationException, IllegalAccessException {
		MergeWorker mw = new MergeWorker(null, null, null);
		mw.mergeRunsFanIn(runFiles, outf);
		deleteTempFiles(runFiles);
	}

	public void runFanIn(Collection<T> ins, File outf) throws IOException, InstantiationException, IllegalAccessException {
		final Collection<File> runFiles = writeSortedRuns(ins);
		MergeWorker mw = new MergeWorker(null, null, null);
		mw.mergeRunsFanIn(runFiles, outf);
		deleteTempFiles(runFiles);
	}

	/**
	 * No code to limit the fan-in, so this might barf.
	 */
	public void runFanIn(File inf, File outf) throws IOException, InstantiationException, IllegalAccessException {
		final Collection<File> runFiles = writeSortedRuns(inf);
		MergeWorker mw = new MergeWorker(null, null, null);
		mw.mergeRunsFanIn(runFiles, outf);
		deleteTempFiles(runFiles);
	}

	public void run(Collection<T> ins, File outf) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		final Collection<File> runFiles = writeSortedRuns(ins); 
		mergeSequential(runFiles, outf);
		deleteTempFiles(runFiles);
	}
	
	private DataOutputStream getBufferedDataOutputStream(File file) throws FileNotFoundException {
		return new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(file), FAST_BUFFER_SIZE));
	}
	
	private DataInputStream getBufferedDataInputStream(File file) throws FileNotFoundException {
		return new DataInputStream(new FastBufferedInputStream(new FileInputStream(file), FAST_BUFFER_SIZE));
	}
	
	private boolean shouldFlushRun(ArrayList<T> run) {
		if (runSize > 0 && run.size() > runSize) {
			return true;
		}
		// checking RAM load is more aggressive, can turn off if
		// there are other serious contenders for RAM
		MemoryStatus.get(usedMem, availMem, freeMem, totalMem, maxMem);
		final boolean ans = availMem.doubleValue() < minFreeFraction * maxMem.doubleValue();
		return ans; 
	}
	
	private Collection<File> writeSortedRuns(File inf) throws IOException, InstantiationException, IllegalAccessException {
		ArrayList<File> ans = new ArrayList<File>();
		ArrayList<T> run = new ArrayList<T>();
		DataInputStream dis = getBufferedDataInputStream(inf);
		T rec = type.newInstance();
		for (;;) {
			try {
				rec.load(dis);
				if (shouldFlushRun(run)) {
					ans.add(writeOneSortedRun(run));
				}
				T copy = type.newInstance();
				copy.replace(rec);
				run.add(copy);
			}
			catch (EOFException eofx) {
				break;
			}
		}
		if (!run.isEmpty()) {
			ans.add(writeOneSortedRun(run));
		}
		// sentinel empty run
		run.clear();
		ans.add(writeOneSortedRun(run));
		dis.close();
		logger.debug(ans);
		return ans;
	}
	
	public Collection<File> writeSortedRuns(Collection<T> records) throws IOException, InstantiationException, IllegalAccessException {
		ArrayList<File> ans = new ArrayList<File>();
		ArrayList<T> run = new ArrayList<T>();
		for (T record : records) {
			if (shouldFlushRun(run)) {
				ans.add(writeOneSortedRun(run));
			}
			run.add(record);
		}
		if (!run.isEmpty()) {
			ans.add(writeOneSortedRun(run));
		}
		logger.debug(ans);
		return ans;
	}
	
	/**
	 * <b>Not thread-safe!</b>
	 * @param oneRun array of unsorted records in one run
	 * @return file to which sorted run has been saved
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	private File writeOneSortedRun(final ArrayList<T> oneRun) throws IOException, InstantiationException, IllegalAccessException {
		MemoryStatus.get(usedMem, availMem, freeMem, totalMem, maxMem);
		logger.debug("before flush avail=" + availMem.longValue()/(1<<20) + " max=" + maxMem.longValue()/(1<<20));
		runSorter.clear();
		for (int rx = 0, rn = oneRun.size(); rx < rn; ++rx) {
			runSorter.add(rx);
		}
		Sorting.quickSort(runSorter.elements(), 0, runSorter.size(), new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final T rec1 = oneRun.get(o1), rec2 = oneRun.get(o2);
				return comparator.compare(rec1, rec2);
			}
		});
		
//		Collections.sort(oneRun, comparator);

		File runFile = File.createTempFile(type.getSimpleName(), suffix, tempDir);
		DataOutputStream dos = getBufferedDataOutputStream(runFile);
		MergeWorker mw = new MergeWorker(null, null, null);
		logger.debug(oneRun.size() + " records -> " + runFile);
//		for (T rec : oneRun) {
//			logger.trace("\t" + rec);
//			mw.writeObjectCheckUnique(dos, rec);
//		}

		for (int pos : runSorter) {
			final T rec = oneRun.get(pos);
			mw.writeObjectCheckUnique(dos, rec);
		}
		
		dos.close();
		oneRun.clear();
		runSorter.clear();
		System.gc();
		MemoryStatus.get(usedMem, availMem, freeMem, totalMem, maxMem);
		logger.debug("after flush avail=" + availMem.longValue()/(1<<20) + " max=" + maxMem.longValue()/(1<<20));
		return runFile;
	}
	
	public void mergeParallel(Collection<File> inputFiles, File outputFile, int nThreads) throws IOException, InterruptedException, InstantiationException, IllegalAccessException, ExecutionException, ClassNotFoundException {
		if (inputFiles.size() <= 1) {
			mergeSequential(inputFiles, outputFile);
		}
		ReferenceArrayList<File> tempFiles = new ReferenceArrayList<File>();
		ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
		LinkedList<Future<File>> unsyncWork = new LinkedList<Future<File>>();
		for (final File inputFile : inputFiles) {
			unsyncWork.add(executorService.submit(new InitializeWorker(inputFile)));
		}
		while (unsyncWork.size() > 1) {
			final Future<File> run1 = unsyncWork.removeFirst();
			final Future<File> run2 = unsyncWork.removeFirst();
			final File outRun;
			if (unsyncWork.isEmpty()) {
				outRun = outputFile;
			}
			else {
				outRun = File.createTempFile(type.getSimpleName(), suffix, tempDir);
				tempFiles.add(outRun);
			}
			final MergeWorker mw = new MergeWorker(run1, run2, outRun);
			unsyncWork.addLast(executorService.submit(mw));
		}
		unsyncWork.pop().get();
		logger.debug("Root merge task done.");
		executorService.shutdown();
		while (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
			logger.debug("Still waiting for termination...");
		}
		deleteTempFiles(tempFiles);
	}
	
	class InitializeWorker implements Callable<File> {
		final File srcFile;
		protected InitializeWorker(File srcFile) {
			this.srcFile = srcFile;
		}
		@Override
		public File call() throws Exception {
			return srcFile;
		}
	}
	
	class MergeWorker implements Callable<File> {
		final Future<File> run1, run2;
		final File outRun;
		final T lastWrittenTee; 
		boolean isLastWrittenFilled = false;

		MergeWorker(Future<File> run1, Future<File> run2, File outRun) throws IOException, InstantiationException, IllegalAccessException {
			this.lastWrittenTee = type.newInstance();
			this.run1 = run1;
			this.run2 = run2;
			this.outRun = outRun;
		}
		
		@Override
		public File call() throws Exception {
			mergeTwoRuns(run1.get(), run2.get(), outRun);
			return outRun;
		}
		
		/**
		 * @param inRuns Can handle arbitrary fan-in
		 * @param outRun
		 */
		void mergeRunsFanIn(Collection<File> inRuns, File outRun) throws IOException, InstantiationException, IllegalAccessException {
			logger.info("Merge " + inRuns + " into " + outRun);
			final DataOutputStream oos = getBufferedDataOutputStream(outRun);
			final ArrayList<DataInputStream> inDiss = new ArrayList<DataInputStream>();
			for (File inRun : inRuns) {
				inDiss.add(getBufferedDataInputStream(inRun));
			}
			final Reference2ReferenceOpenHashMap<T, DataInputStream> headToRest = new Reference2ReferenceOpenHashMap<T, DataInputStream>();
			final ObjectHeapPriorityQueue<T> mergeHeap = new ObjectHeapPriorityQueue<T>(comparator);
			for (DataInputStream inDis : inDiss) {
				final T rec = type.newInstance();
				if (readObjectNoEOF(inDis, rec)) {
					mergeHeap.enqueue(rec);
					headToRest.put(rec, inDis);
				}
			}
			isLastWrittenFilled = false;
			while (!mergeHeap.isEmpty()) {
				final T first = mergeHeap.dequeue();
				writeObjectCheckUnique(oos, first);
				if (readObjectNoEOF(headToRest.get(first), first)) {
					mergeHeap.enqueue(first);
				}
				else {
					headToRest.remove(first); // contents of first may be mangled but we are using a reference based map 
				}
			}
			for (DataInputStream inDis : inDiss) {
				inDis.close();
			}
			oos.close();
		}
		
		/**
		 * We continue to use input parameters so that {@link MergeWorker} can also
		 * be used for sequential merging. 
		 */
		void mergeTwoRuns(File run1, File run2, File outRun) throws IOException, InstantiationException, IllegalAccessException {
			logger.info("merge " + run1 + ", " + run2 + " to " + outRun);
			isLastWrittenFilled = false;
			DataInputStream ois1 = getBufferedDataInputStream(run1);
			DataInputStream ois2 = getBufferedDataInputStream(run2);
			DataOutputStream oos = getBufferedDataOutputStream(outRun);
			T com1 = type.newInstance(), com2 = type.newInstance();
			boolean ok1 = readObjectNoEOF(ois1, com1), ok2 = readObjectNoEOF(ois2, com2);
			for (;;) {
				if (!ok1 && !ok2) {
					break;
				}
				else if (!ok1 && ok2) {
					writeObjectCheckUnique(oos, com2);
					ok2 = readObjectNoEOF(ois2, com2);
				}
				else if (ok1 && !ok2) {
					writeObjectCheckUnique(oos, com1);
					ok1 = readObjectNoEOF(ois1, com1);
				}
				else {
					final int comparison = comparator.compare(com1, com2);
					if (comparison < 0) {
						writeObjectCheckUnique(oos, com1);
						ok1 = readObjectNoEOF(ois1, com1);
					}
					else if (comparison > 0) {
						writeObjectCheckUnique(oos, com2);
						ok2 = readObjectNoEOF(ois2, com2);
					}
					else {
						writeObjectCheckUnique(oos, com1);
						writeObjectCheckUnique(oos, com2);
						ok1 = readObjectNoEOF(ois1, com1);
						ok2 = readObjectNoEOF(ois2, com2);
					}
				}
			}
			ois1.close();
			ois2.close();
			oos.close();
		}
		
		void writeObjectCheckUnique(DataOutputStream oos, T next) throws IOException {
			if (!doUnique || !isLastWrittenFilled || comparator.compare(lastWrittenTee, next) != 0) {
				logger.trace("w\t" + next);
				next.store(oos);
				lastWrittenTee.replace(next);
				isLastWrittenFilled = true;
			}
			else {
				logger.trace("0\t" + next);
			}
		}
		
		boolean readObjectNoEOF(DataInputStream ois, T rec) throws IOException {
			try {
				rec.load(ois);
				logger.trace("r\t" + rec);
				return true;
			}
			catch (EOFException eofx) {
				return false;
			}
		}

		void copyOneRun(File inFile, File outFile) throws IOException, InstantiationException, IllegalAccessException {
			isLastWrittenFilled = false;
			final DataInputStream dis = getBufferedDataInputStream(inFile);
			final DataOutputStream dos = getBufferedDataOutputStream(outFile);
			final T com = type.newInstance();
			while (readObjectNoEOF(dis, com)) {
				writeObjectCheckUnique(dos, com);
			}
			dis.close();
			dos.close();
		}
	}
	
	private void deleteTempFiles(Collection<File> tempFiles) {
		for (File tempFile : tempFiles) {
			logger.debug("Deleting " + tempFile);
			final boolean didRemove = tempFile.delete();
			if (!didRemove) {
				logger.warn("Could not delete " + tempFile);
			}
		}
	}
	
	public void mergeSequential(Collection<File> inputFiles, File outputFile) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		final ReferenceArrayList<File> tempFiles = new ReferenceArrayList<File>();
		if (inputFiles.size() == 1) {
			MergeWorker mw = new MergeWorker(null, null, null);
			mw.copyOneRun(inputFiles.iterator().next(), outputFile);
		}
		LinkedList<File> mergeQueue = new LinkedList<File>(inputFiles);
		while (mergeQueue.size() > 1) {
			File run1 = mergeQueue.removeFirst();
			File run2 = mergeQueue.removeFirst();
			File outRun = null;
			if (mergeQueue.isEmpty()) {
				outRun = outputFile;
			}
			else {
				outRun = File.createTempFile(type.getSimpleName(), suffix, tempDir);
				tempFiles.add(outRun);
			}
			MergeWorker mw = new MergeWorker(null, null, null);
			mw.mergeTwoRuns(run1, run2, outRun);
			mergeQueue.addLast(outRun);
		}
		deleteTempFiles(tempFiles);
	}
}
