package iitb.CSAW.Utils.Sort;

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

public class BitExternalMergeReduce<Tin extends IBitRecord<Tin>, Tout extends IBitRecord<Tout>> extends BitExternalMergeSort<Tin> {
	final Class<? extends Tout> typeOut;
	
	public BitExternalMergeReduce(Class<? extends Tin> blah, Class<? extends Tout> blah2, File tmpDir) throws InstantiationException, IllegalAccessException {
		super(blah, tmpDir);
		this.typeOut = blah2;
	}
	
	/**
	 * @param iFiles input runs, each in increasing key order
	 * @param reducer
	 * @param oFile save output to this file
	 */
	public void reduceUsingHeap(Collection<File> iFiles, ABitReducer<Tin, Tout> reducer, File oFile) throws IOException, InstantiationException, IllegalAccessException {
		ProgressLogger plin = new ProgressLogger(logger), plout = new ProgressLogger(logger);
		plin.logInterval = plout.logInterval = ProgressLogger.ONE_MINUTE;
		plin.itemsName = "inputRecords";
		plout.itemsName = "outputRecords";
		plin.start();
		plout.start();
		final Reference2ReferenceOpenHashMap<Tin, InputBitStream> headToRest = new Reference2ReferenceOpenHashMap<Tin, InputBitStream>();
		final ObjectHeapPriorityQueue<Tin> mergeHeap = new ObjectHeapPriorityQueue<Tin>(comparator);
		for (File inRun : iFiles) {
			InputBitStream ibs = getBufferedInputBitStream(inRun);
			Tin rec = type.newInstance();
			try {
				rec.load(ibs);
				headToRest.put(rec, ibs);
				mergeHeap.enqueue(rec);
			}
			catch (EOFException eofx) {
				ibs.close(); // and skip this stream
			}
		}
		final OutputBitStream obs = getBufferedOutputBitStream(oFile);
		reducer.reset();
		final Tout outrec = typeOut.newInstance();
		while (!mergeHeap.isEmpty()) {
			final Tin first = mergeHeap.dequeue();
			plin.update();
			logger.trace("\tdequeued " + first);
			final int keyComp = reducer.compareKeys(first);
			if (keyComp > 0) {
				reducer.getResult(outrec);
				final String message = outrec + " :: " + first;
				logger.fatal(message);
				throw new IllegalStateException(message);
			}
			else {
				if (keyComp == 0) {
					reducer.accumulate(first);
				}
				else { // keyComp < 0
					reducer.getResult(outrec);
					if (!outrec.isNull()) {
						outrec.store(obs);
						plout.update();
					}
					reducer.reset();
					reducer.accumulate(first);
				}
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
		} // while heap not empty
		reducer.getResult(outrec);
		if (!outrec.isNull()) {
			outrec.store(obs);
			plout.update();
		}
		obs.close();
		if (!headToRest.isEmpty()) {
			final String message = "Merge heap is empty but headToRest is not!"; 
			logger.fatal(message);
			throw new IllegalStateException(message);
		}
		plin.stop();
		plin.done();
		plout.stop();
		plout.done();
	}

	/* ---------------- Testing code ---------------- */
	
	static class WordCount implements IBitRecord<WordCount>{
		MutableString word = new MutableString();
		long count;
		@Override
		public String toString() {
			if (isNull()) return "null";
			return word + ":" + count;
		}
		@Override
		public Comparator<WordCount> getComparator() {
			return new Comparator<WordCount>() {
				@Override
				public int compare(WordCount o1, WordCount o2) {
					return o1.word.compareTo(o2.word);
				}
			};
		}
		@Override
		public boolean isNull() {
			return word.length() == 0 && count == 0;
		}
		@Override
		public void setNull() {
			word.length(0);
			count = 0;
		}
		@Override
		public void replace(WordCount ibr) {
			word.replace(ibr.word);
			count = ibr.count;
		}
		@Override
		public void store(OutputBitStream obs) throws IOException {
			obs.writeInt(word.length(), Integer.SIZE);
			for (int wx = 0, wn = word.length(); wx < wn; ++wx) {
				obs.writeInt(word.charAt(wx), Character.SIZE);
			}
			obs.writeLong(count, Long.SIZE);
		}
		@Override
		public void load(InputBitStream ibs) throws IOException {
			word.length(ibs.readInt(Integer.SIZE));
			for (int wx = 0, wn = word.length(); wx < wn; ++wx) {
				word.setCharAt(wx, (char) ibs.readInt(Character.SIZE));
			}
			count = ibs.readLong(Long.SIZE);
		}
	}

	static class WordCountReducer extends ABitReducer<WordCount, WordCount> {
		private WordCount wc = new WordCount();
		@Override
		public void reset() {
			wc.setNull();
		}
		@Override
		public void accumulate(WordCount inrec) {
			if (wc.isNull()) {
				wc.replace(inrec);
			}
			else {
				if (compareKeys(inrec) != 0) {
					throw new IllegalArgumentException(wc + " :: " + inrec + " = " + compareKeys(inrec));
				}
				wc.count += inrec.count;
			}
		}
		@Override
		public int compareKeys(WordCount inrec) {
			if (wc.isNull()) return -1;
			return wc.word.compareTo(inrec.word);
		}
		@Override
		public void getResult(WordCount outrec) {
			outrec.replace(wc);
		}
	}
	
	static void createRun(String fname, String... words) throws IOException {
		OutputBitStream obs = getBufferedOutputBitStream(new File(fname));
		WordCount wc = new WordCount();
		for (String word : words) {
			wc.word.replace(word);
			wc.count = 1;
			wc.store(obs);
		}
		obs.close();
	}
	
	static void printRun(String fname) throws IOException {
		System.out.println(fname);
		InputBitStream ibs = getBufferedInputBitStream(new File(fname));
		for (;;) {
			WordCount wc = new WordCount();
			try {
				wc.load(ibs);
				System.out.println("\t" + wc);
			}
			catch (EOFException xeof) {
				break;
			}
		}
		ibs.close();
	}
	
	public static void main(String[] args) throws Exception {
		createRun("/tmp/if1.dat", "aardvark", "arkansas", "bombay", "delaware", "zimbabwe");
		printRun("/tmp/if1.dat");
		createRun("/tmp/if2.dat", "bombay", "zimbabwe", "zulu");
		printRun("/tmp/if2.dat");
		BitExternalMergeReduce<WordCount, WordCount> bemr = new BitExternalMergeReduce<WordCount, WordCount>(WordCount.class, WordCount.class, new File("/tmp"));
		bemr.reduceUsingHeap(Arrays.asList(new File("/tmp/if1.dat"), new File("/tmp/if2.dat")), new WordCountReducer(), new File("/tmp/of0.dat"));
		printRun("/tmp/of0.dat");
	}
}
