package iitb.CSAW.Spotter;

import com.g414.hash.LongHash;
import com.g414.hash.impl.JenkinsHash;

public class SignedHashFunction {
	private static final long maskLow = 0x3fffffffffffffffL, maskHigh = 0x4000000000000000L;
	private final long nBuckets;
	
	public final LongHash longHash = new JenkinsHash(); // MurmurHash();
	
	public long bucket;
	public int sign;
	
	public SignedHashFunction(long nBuckets) {
		this.nBuckets = nBuckets;
	}
	
	/**
	 * Updates {@link #bucket} and {@link #sign}. 
	 * @param in content to hash
	 */
	public void doHash(byte[] in) {
		final long longHashCode = longHash.getLongHashCode(in);
		bucket = (maskLow & longHashCode) % nBuckets;
		sign = (longHashCode & maskHigh) == 0? 1 : -1;
	}
	
	public static void pack(byte[] ibuf, int ia) {
		ibuf[0] = (byte) ((ia & 0xff));
		ibuf[1] = (byte) ((ia & 0xff00) >>> 8);
		ibuf[2] = (byte) ((ia & 0xff0000) >>> 16);
		ibuf[3] = (byte) ((ia & 0xff000000) >>> 24);
	}
	
	public static void pack(byte[] out, long la) {
		out[0] = (byte) ((la & 0xffL));
		out[1] = (byte) ((la & 0xff00L) >>> 8);
		out[2] = (byte) ((la & 0xff0000L) >>> 16);
		out[3] = (byte) ((la & 0xff000000L) >>> 24);
		out[4] = (byte) ((la & 0xff00000000L) >>> 32);
		out[5] = (byte) ((la & 0xff0000000000L) >>> 40);
		out[6] = (byte) ((la & 0xff000000000000L) >>> 48);
		out[7] = (byte) ((la & 0xff00000000000000L) >>> 56);
	}

	public static void pack(byte[] out, int leaf, int feat, int ent) {
		out[0] = (byte) ((leaf & 0xff));
		out[1] = (byte) ((leaf & 0xff00) >>> 8);
		out[2] = (byte) ((leaf & 0xff0000) >>> 16);
		out[3] = (byte) ((leaf & 0xff000000) >>> 24);
		out[4] = (byte) ((feat & 0xff));
		out[5] = (byte) ((feat & 0xff00) >>> 8);
		out[6] = (byte) ((feat & 0xff0000) >>> 16);
		out[7] = (byte) ((feat & 0xff000000) >>> 24);
		out[8] = (byte) ((ent & 0xff));
		out[9] = (byte) ((ent & 0xff00) >>> 8);
		out[10] = (byte) ((ent & 0xff0000) >>> 16);
		out[11] = (byte) ((ent & 0xff000000) >>> 24);
	}
}
