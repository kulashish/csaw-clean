package iitb.CSAW.Utils;

/**
 *  A subclass of RandomAccessFile to enable basic buffering to a byte array
 *  Copyright (C) 2009 minddumped.blogspot.com

 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.

 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.

 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see .
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.commons.lang.NotImplementedException;

/**
 * 
 * @author minddumped.blogspot.com
 */
public class BufferedRaf extends RandomAccessFile {

	private byte[] bytebuffer;
	private int maxread;
	private int bufferlength;
	private int buffpos;
	private StringBuilder sb;
	private boolean stale;

	public BufferedRaf(File file, String mode, int bufferlength)
			throws FileNotFoundException {
		super(file, mode);
		this.bufferlength = bufferlength;
		bytebuffer = new byte[bufferlength];
		maxread = 0;
		buffpos = 0;
		stale = false;
		sb = new StringBuilder("0");
	}

	public BufferedRaf(File file, String mode) throws FileNotFoundException {
		this(file, mode, 65536);
	}

	public BufferedRaf(String file, String mode, int bufferlength)
			throws FileNotFoundException {
		this(new File(file), mode, bufferlength);
	}

	public BufferedRaf(String file, String mode) throws FileNotFoundException {
		this(new File(file), mode);
	}

	public int getbuffpos() {
		return buffpos;
	}

	public String readString() throws IOException {
		int len = readInt();
		if (len < 0) {
			System.out.println(len);
		}
		char[] chars = new char[len];
		for (int i = 0; i < len; i++)
			chars[i] = readChar();
		return new String(chars);
	}

	public void writeString(String s) throws IOException {
		int len = s.length();
		writeInt(len);
		for (int i = 0; i < len; i++)
			writeChar(s.charAt(i));
	}

	@Override
	public int read() throws IOException {
		if (buffpos >= maxread) {
			writeStale();
			maxread = readchunk();
			if (maxread == -1) {
				return -1;
			}
		}
		buffpos++;
		return bytebuffer[buffpos - 1] & 0xFF;
	}

	public void write(int b) throws IOException {
		if (buffpos >= bufferlength) {
			writeStale();
			maxread = readchunk();
		}
		bytebuffer[buffpos] = (byte) (b & 0xFF);
		buffpos++;
		stale = true;
		if (buffpos > maxread) {
			if (maxread < bufferlength)
				maxread = buffpos;
		}
	}

	/**
	 * Uses string builder, hence faster
	 * 
	 * @return
	 * @throws IOException
	 */
	public String readLine2() throws IOException {
		sb.delete(0, sb.length());
		int c = -1;
		boolean eol = false;
		while (!eol) {
			switch (c = read()) {
			case -1:
			case '\n':
				eol = true;
				break;
			case '\r':
				eol = true;
				long cur = getFilePointer();
				if ((read()) != '\n') {
					seek(cur);
				}
				break;
			default:
				sb.append((char) c);
				break;
			}
		}

		if ((c == -1) && (sb.length() == 0)) {
			return null;
		}
		return sb.toString();
	}

	@Override
	public long getFilePointer() throws IOException {
		return super.getFilePointer() + buffpos;
	}

	// TODO FIX ME for negative seeks
	@Override
	public void seek(long pos) throws IOException {
		long curr = super.getFilePointer();
		if (pos >= curr) {
			if (maxread != -1 && pos < (curr + maxread)) {
				Long diff = (pos - curr);
				if (diff < Integer.MAX_VALUE) {
					buffpos = diff.intValue();
				} else {
					throw new IOException("something wrong w/ seek");
				}
			} else {
				writeStale();
				buffpos = 0;
				super.seek(pos);
				maxread = readchunk();
			}
		} else {

			System.out
					.println("BufferedRaf.seek() Does not works for negative seeks");
			throw new NotImplementedException();
			// writeStale();
			// buffpos = 0;
			// super.seek(pos);
			// maxread = readchunk();
		}
	}

	public void close() throws IOException {
		writeStale();
		super.close();
	}

	private void writeStale() throws IOException {
		if (stale) {
			long pos = super.getFilePointer();
			super.write(bytebuffer, 0, buffpos);
			super.seek(pos);
			stale = false;
		}
	}

	private int readchunk() throws IOException {
		stale = false;
		long pos = super.getFilePointer() + buffpos;
		super.seek(pos);
		int read = super.read(bytebuffer);
		super.seek(pos);
		buffpos = 0;
		return read;
	}

	// public long indexOf(char target[]) {
	// int targetOffset = 0;
	// char first = target[targetOffset];
	//
	// long prev;
	// char ch;
	// try {
	// long start = getFilePointer();
	// while (true) {
	// /* Look for first character. */
	// while (true) {
	// prev = getFilePointer();
	// ch = (char) read();
	// if (ch == first) {
	// break;
	// }
	// }
	// long prevPlusOne = getFilePointer();
	// /* Found first character, now look at the rest of v2 */
	// boolean sucess = true;
	// for (int i = 1; i < target.length; i++) {
	// ch = (char) read();
	// if (ch != target[i]) {
	// sucess = false;
	// break;
	// }
	// }
	// if (sucess) {
	// seek(start);
	// return prev;
	// } else {
	// seek(prevPlusOne);
	// }
	// }
	// } catch (EOFException e) {
	// return -1;
	// } catch (IOException e) {
	// e.printStackTrace();
	// }
	// return -1;
	// }

	public static void main(String[] args) throws IOException {

		BufferedRaf br = new BufferedRaf("test", "rw");
		br.writeString("side");
		br.writeString("-1");
		br.writeInt(10000000);
		br.seek(0);
		System.out.println(br.readString());
		System.out.println(br.readString());
		System.out.println(br.readInt());
	}
}