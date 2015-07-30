package iitb.CSAW.Utils;

import iitb.CSAW.Spotter.ContextRecord;

import java.nio.ByteBuffer;

/**
 * Pack, unpack and compare two ints in a long.
 * Each instance to be used from only one thread.
 * 
 * @author soumen
 * 
 * @since 2011/07/07 Separated out from {@link ContextRecord} and
 * replaced {@link ByteBuffer} methods with bit shifts. Old methods should
 * be removed after testing the new code.
 */
public class LongIntInt implements Comparable<LongIntInt> {
	private ByteBuffer bb = ByteBuffer.allocate(Long.SIZE/Byte.SIZE);
	public long lv;
	public int iv1, iv0;
	
	public void write1(long olv) {
		lv = olv;
		bb.putLong(0, lv);
		bb.rewind();
		iv1 = bb.getInt();
		iv0 = bb.getInt();
	}
	
	public void write(long olv) {
		lv = olv;
		iv1 = (int) ( (lv >>> 32) & 0x00000000ffffffffL);
		iv0 = (int) ( (lv & 0x00000000ffffffffL) );
	}
	
	public void write1(int oiv1, int oiv0) {
		bb.rewind();
		iv1 = oiv1;
		bb.putInt(oiv1);
		iv0 = oiv0;
		bb.putInt(oiv0);
		lv = bb.getLong(0);
	}
	
	public void write(int oiv1, int oiv0) {
		iv1 = oiv1;
		iv0 = oiv0;
		lv = (((long) iv1) << 32) | (((long) iv0) & 0x00000000ffffffffL);
	}
	
	@Override
	public int compareTo(LongIntInt o) {
		final int cmp1 = iv1 - o.iv1;
		if (cmp1 != 0) return cmp1;
		return iv0 - o.iv0;
	}
	
	static void test1(LongIntInt lii, long lv) {
		lii.write(lv);
		System.out.println(String.format("%x == %x %x", lii.lv, lii.iv1, lii.iv0));
	}
	
	static void test2(LongIntInt lii, int iv1, int iv0) {
		lii.write(iv1, iv0);
		System.out.println(String.format("%x == %x %x", lii.lv, lii.iv1, lii.iv0));
	}
	
	public static void main(String[] args) {
		LongIntInt lii = new LongIntInt();
		test1(lii, 0x1234567876543210L);
		test2(lii, 0x12345678, 0x76543210);
		test1(lii, 0xfafafafaeaeaeaeaL);
		test2(lii, 0xdadadada, 0xfafafafa);
	}
}
