package iitb.CSAW.Utils;

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;

/**
 * Adapted from a similar class in GUAVA.
 * @author soumen
 */
public class ReusableByteArrayDataOutput implements DataOutput {
    protected FastByteArrayOutputStream baos = new FastByteArrayOutputStream();
	/** Allowed to wrap around without checks. */
    protected long longWritten = 0;
    protected DataOutputStream dos = null;
	
	public ReusableByteArrayDataOutput() {
		reset();
	}
	
	public void flush() throws IOException {
		dos.flush();
		baos.flush();
	}
	
	public void reset() {
		longWritten = 0;
		baos.reset();
		dos = new DataOutputStream(baos);
	}
	
	public FastByteArrayOutputStream get() throws IOException {
		flush();
		return baos;
	}

    public final long longSize() {
    	return longWritten;
    }

	@Override
	public void write(int b) throws IOException {
		dos.write(b);
		longWritten += 1;
	}

	@Override
	public void write(byte[] b) throws IOException {
		dos.write(b);
		longWritten += b.length;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		dos.write(b, off, len);
		longWritten += len;
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		dos.writeBoolean(v);
		longWritten += 1;
	}

	@Override
	public void writeByte(int v) throws IOException {
		dos.writeByte(v);
		longWritten += 1;
	}

	@Override
	public void writeBytes(String s) throws IOException {
		dos.writeBytes(s);
		longWritten += s.length();
	}

	@Override
	public void writeChar(int v) throws IOException {
		dos.writeChar(v);
		longWritten += 2;
	}

	@Override
	public void writeChars(String s) throws IOException {
		dos.writeChars(s);
		longWritten += 2 * s.length();
	}

	@Override
	public void writeDouble(double v) throws IOException {
		dos.writeDouble(v);
		longWritten += 8;
	}

	@Override
	public void writeFloat(float v) throws IOException {
		dos.writeFloat(v);
		longWritten += 4;
	}

	@Override
	public void writeInt(int v) throws IOException {
		dos.writeInt(v);
		longWritten += 4;
	}

	@Override
	public void writeLong(long v) throws IOException {
		dos.writeLong(v);
		longWritten += 8;
	}

	@Override
	public void writeShort(int v) throws IOException {
		dos.writeShort(v);
		longWritten += 2;
	}

	@Override
	public void writeUTF(String s) throws IOException {
		throw new NotImplementedException();
	}

	/**
	 * Test harness.
	 * @param args None required.
	 * @throws Exception
	 */
    public static final void main(String[] args) throws Exception {
    	ReusableByteArrayDataOutput dos = new ReusableByteArrayDataOutput();
    	dos.writeBoolean(false);
    	dos.writeShort((short) 37);
    	for (char c : "Mary had a little lamb".toCharArray()) {
    		dos.writeChar(c);
    	}
    	System.out.println(dos.longSize());
    	// check conventional unpacking
    	final FastByteArrayOutputStream fbaos = dos.get();
    	FastByteArrayInputStream fbais = new FastByteArrayInputStream(fbaos.array, 0, fbaos.length);
    	DataInputStream dis = new DataInputStream(fbais);
    	System.out.println(dis.readBoolean());
    	System.out.println(dis.readShort());
    	for (int cx = 0; cx < 10; ++cx) {
    		System.out.print(dis.readChar());
    	}
    	System.out.println();
    	dos.reset();
    	for (char c : "and I ate it!".toCharArray()) {
    		dos.writeChar(c);
    	}
    	System.out.println(dos.longSize());
    }
}
