package iitb.CSAW.Utils.Sort;

import iitb.CSAW.Index.SIP1.Sip1Document;
import iitb.CSAW.Index.SIP1.Sip1IndexWriter;
import iitb.CSAW.Index.SIP2.Sip2Document;
import iitb.CSAW.Index.SIP2.Sip2IndexWriter;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Comparator;

import cern.colt.Sorting;
import cern.colt.function.LongComparator;

import com.google.common.io.PatternFilenameFilter;

/**
 * <p>{@link Sip1IndexWriter} and {@link Sip2IndexWriter} used custom code to 
 * accumulate and write out sorted runs of {@link Sip1Document}s and 
 * {@link Sip2Document}s. Here we seek to avoid that. This class buffers
 * arbitrary SIP records implementing {@link IBitRecord}, sorts by 
 * reference, and writes out a sorted run. Not thread-safe.</p>
 * 
 * <p>Construct an instance and keep calling {@link #append(IBitRecord)}
 * until it returns {@link Boolean#<code>false</code>}.  Then call 
 * {@link #flushSortedRun(OutputBitStream)} giving a suitable output stream.</p>
 * 
 * <p><b>Bug note:</b> {@link OutputBitStream#position(long)}
 * in dsiutils-1.0.10 and earlier has a critical bug that you need to fix
 * before you use this class.</p> 
 * 
 * @author soumen
 * @since 2011/05/08
 *
 * @param <T> the type of records
 */

public class BitSortedRunWriter<T extends IBitRecord<T>> {
	static final int fileBufSize = 8192;
	static final double runMaxFill = 0.9;
	final byte[] runBuf;
	final OutputBitStream runObs;
	final long maxWrittenBits;
	final Class<T> type;
	final Comparator<T> comp;
	final LongArrayList ofs = new LongArrayList();
	
	public BitSortedRunWriter(Class<T> type, int runBytes) throws InstantiationException, IllegalAccessException {
		this.type = type;
		runBuf = new byte[runBytes];
		runObs = new OutputBitStream(runBuf);
		maxWrittenBits = (long) (runMaxFill * runBytes * Byte.SIZE);
		comp = type.newInstance().getComparator();
	}
	
	public ArrayList<File> writeRuns(File runDir, File in) throws InstantiationException, IllegalAccessException, IOException {
		final ArrayList<File> newRunFiles = new ArrayList<File>();
		for (File oldRunFile : runDir.listFiles(new PatternFilenameFilter(type.getCanonicalName() + "_\\d+.dat"))) {
			if (!oldRunFile.delete()) {
				throw new IOException("Cannot delete " + oldRunFile);
			}
		}
		int runCounter = 0;
		final InputBitStream ibs = new InputBitStream(in, fileBufSize);
		final T rec = type.newInstance();
		for (;;) {
			try {
				rec.load(ibs);
				if (!append(rec)) {
					final File newRunFile = new File(runDir, type.getCanonicalName() + "_" + (runCounter++) + ".dat");
					newRunFiles.add(newRunFile);
					final OutputBitStream newRunObs = new OutputBitStream(newRunFile, fileBufSize);
					flushSortedRun(newRunObs);
					newRunObs.close();
				}
			}
			catch (EOFException eofx) {
				final File newRunFile = new File(runDir, type.getCanonicalName() + "_" + (runCounter++) + ".dat");
				newRunFiles.add(newRunFile);
				final OutputBitStream newRunObs = new OutputBitStream(newRunFile, fileBufSize);
				flushSortedRun(newRunObs);
				newRunObs.close();
				break;
			}
		}
		ibs.close();
		return newRunFiles;
	}
	
	/**
	 * @param rec record
	 * @return false if buffer full or error, true if appended to buffer
	 * @throws IOException
	 */
	public boolean append(T rec) throws IOException {
		final long preWrittenBits = runObs.writtenBits();
		if (preWrittenBits >= maxWrittenBits) {
			return false;
		}
		try {
			rec.store(runObs);
			return true;
		} catch (IOException iox) { // rewind
			runObs.writtenBits(preWrittenBits);
			runObs.position(preWrittenBits);
			return false;
		}
	}
	
	public void flushSortedRun(OutputBitStream fobs) throws IOException , InstantiationException, IllegalAccessException{
		runObs.flush();
		sortRun();
		writeRun(fobs);
		runObs.position(0);
		runObs.writtenBits(0);
		ofs.clear();
	}
	
	void sortRun() throws InstantiationException, IllegalAccessException, IOException {
		runObs.flush();
		ofs.clear();
		// note that we need to keep track of available bits separately
		final long availableBits = runObs.writtenBits();
		final InputBitStream ibs = new InputBitStream(runBuf);
		ibs.position(0);
		final T rec = type.newInstance();
		for (; ibs.readBits() < availableBits; ) {
			final long preRead = ibs.readBits();
			rec.load(ibs); // should not throw EOF or IO exception
			ofs.add(preRead);
		}
		Sorting.quickSort(ofs.elements(), 0, ofs.size(), new LongComparator() {
			final T rec1 = type.newInstance(), rec2 = type.newInstance();
			@Override
			public int compare(long o1, long o2) {
				try {
					randomRead(ibs, o1, rec1);
					randomRead(ibs, o2, rec2);
					return comp.compare(rec1, rec2);
				}
				catch (IOException iox) {
					throw new RuntimeException(iox);
				}
			}
		});
		ibs.close();
	}
	
	void randomRead(InputBitStream ibs, long bitOfs, IBitRecord<T> rec) throws IOException {
		ibs.position(bitOfs);
		rec.load(ibs);
	}
	
	void printRun(PrintStream ps) throws InstantiationException, IllegalAccessException, IOException {
		final InputBitStream ibs = new InputBitStream(runBuf);
		final T rec = type.newInstance(); 
		for (long of : ofs) {
			randomRead(ibs, of, rec);
			ps.println(rec);
		}
	}

	void writeRun(OutputBitStream fobs) throws InstantiationException, IllegalAccessException, IOException {
		final InputBitStream ibs = new InputBitStream(runBuf);
		final T rec = type.newInstance(); 
		for (long of : ofs) {
			randomRead(ibs, of, rec);
			rec.store(fobs);
		}
	}
}
