/**
 * 
 */
package iitb.CSAW.Utils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * Limits a file to a byte range and implements an {@link InputStream} over
 * this range view. Must have been written millions of times over.
 * 
 * @author soumen
 */
public class RangeInputStream extends InputStream {
	final int LEN = 4096;
	final byte[] buf = new byte[LEN];
	final long begin, end;
	long base;
	int cur, max;
	final RandomAccessFile raf;
	
	public RangeInputStream(File file, long begin, long end) throws IOException {
		checkSanity(file, begin, end);
		this.begin = begin;
		this.end = end;
		raf = new RandomAccessFile(file, "r");
		raf.seek(begin);
		fill(begin);
	}
	
	void checkSanity(File file, long begin, long end) {
		assert begin != Long.MIN_VALUE : "begin=" + begin;
		assert end != Long.MIN_VALUE : "end=" + end;
		assert begin <= end : "begin=" + begin + " > end=" + end;
		assert begin < file.length() : "begin=" + begin + " >= length=" + file.length();
		assert end <= file.length() : "end=" + end + " > length=" + file.length();
	}
	
	void fill(long beg) throws IOException {
		base = beg;
		final long span = end - beg;
		assert span >= 0 : "Bad span beg=" + beg + " end=" + end;
		max = (span >= LEN)? LEN : (int) span;
		if (max > 0) {
			final int read = raf.read(buf, 0, max);
			max = (read < 0)? 0 : read;
		}
		cur = 0;
	}
	
	@Override
	public int read() throws IOException {
		if (cur < max) {
			return buf[cur++] & 0xff;
		}
		fill(base+max);
		if (cur < max) {
			return buf[cur++] & 0xff;
		}
		return -1;
	}
	
	@Override
	public void close() throws IOException {
		raf.close();
		super.close();
	}
}