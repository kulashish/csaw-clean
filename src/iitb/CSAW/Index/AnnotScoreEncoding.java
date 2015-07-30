package iitb.CSAW.Index;

import java.util.Arrays;

/**
 * Packs log probability of annotations into one byte to include in SIP index.
 * The log probability is a float, not a double. Given the very crude processes
 * determining these probabilities, a double is not expected to buy additional
 * ranking quality.
 *  
 * @author soumen
 */
public class AnnotScoreEncoding {
	/** Used like a serialVersionUUID to make sure we read right. */
	public static final int version = 1;
	public static final int encodedBits = Byte.SIZE;
	static final int maxUnsignedByte = (1<<Byte.SIZE) - 1;
	static final float steepNess = 2;
	static final float byteToDouble = (float) (Math.log(Float.MAX_VALUE) / Math.pow(maxUnsignedByte, steepNess));
	static final float[] table = new float[1 + maxUnsignedByte];

	/**
	 * The transfer function implemented is
	 * (in^{@link #steepNess}) * {@link #byteToDouble}
	 */
	public AnnotScoreEncoding() {
		for (int bx = 0; bx <= maxUnsignedByte; ++bx) {
			final int i_logNormalizedProb = maxUnsignedByte & bx;
			final float ans = (float) (- Math.pow(i_logNormalizedProb, steepNess) * byteToDouble);
			table[maxUnsignedByte - bx] = ans;
//			System.out.println(bx + " " + ans);
		}
	}

	/**
	 * @param logNormalizedProb byte with value -128..127, will be transformed into 0..255
	 * @return log normalized probability approximation as a double
	 */
	public float decode(int logNormalizedProb) {
		return table[maxUnsignedByte & logNormalizedProb];
	}
	
	/**
	 * @param logNormalizedProb
	 * @return an integer value between 0 and {@link #maxUnsignedByte}
	 */
	public int encode(float logNormalizedProb) {
		int rawFind = Arrays.binarySearch(table, logNormalizedProb);
		if (rawFind >= 0 && rawFind <= maxUnsignedByte) {
			return rawFind;
		}
		if (rawFind < 0) {
			rawFind = -rawFind;
		}
		if (rawFind > maxUnsignedByte) {
			rawFind = maxUnsignedByte;
		}
		return rawFind;
	}
	
	/**
	 * Test harness
	 * @param args non-positive 
	 */
	public static void main(String[] args) {
		AnnotScoreEncoding ase = new AnnotScoreEncoding();
		for (String av : args) {
			final float ad = Float.parseFloat(av);
			System.out.println("av " + ad + " " + ase.encode(ad) + " " + ase.decode(ase.encode(ad)));
		}
	}
}
