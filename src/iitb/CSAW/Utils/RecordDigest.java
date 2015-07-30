package iitb.CSAW.Utils;

import java.nio.ByteBuffer;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import cern.colt.list.ByteArrayList;

/**
 * A digest for records with primitive member types. Inside a record, order
 * of members is significant. Across records, order is not important. The
 * pattern of use is to append various members in a fixed sequence, then end
 * the record. After all records are processed, read off the checksum.
 * 
 * @author soumen
 */
public class RecordDigest {
	private final MessageDigest md;
	private final ByteBuffer bb;
	final byte[] recordDigest, sequenceDigest;
	
	public RecordDigest() throws NoSuchAlgorithmException {
		md = MessageDigest.getInstance("SHA");
		md.reset();
		recordDigest = new byte[md.getDigestLength()];
		sequenceDigest = new byte[md.getDigestLength()];
		bb = ByteBuffer.allocate(2 * Long.SIZE/Byte.SIZE);
	}
	
	public int getDigestLength() {
		return md.getDigestLength();
	}
	
	public void appendByte(byte val) {
		md.update(val);
	}
	
	public void appendChar(char val) {
		bb.putChar(0, val);
		bb.position(0);
		md.update(bb);
	}
	
	public void appendShort(short val) {
		bb.putShort(0, val);
		bb.position(0);
		md.update(bb);
	}
	
	public void appendInt(int val) {
		bb.putInt(0, val);
		bb.position(0);
		md.update(bb);
	}
	
	public void appendLong(long val) {
		bb.putLong(0, val);
		bb.position(0);
		md.update(bb);
	}
	
	public void endRecord() throws DigestException {
		md.digest(recordDigest, 0, recordDigest.length);
		for (int bx = 0, bn = recordDigest.length; bx < bn; ++bx) {
			sequenceDigest[bx] ^= recordDigest[bx];
		}
		md.reset();
	}
	
	public byte[] getDigest() {
		return sequenceDigest;
	}
	
	public static void main(String[] args) throws Exception {
		RecordDigest pd = new RecordDigest();

		pd.appendInt(8);
		pd.appendInt(7);
		pd.endRecord();

		pd.appendInt(5);
		pd.appendInt(2);
		pd.endRecord();

		System.out.println(new ByteArrayList(pd.sequenceDigest));
	}
}
