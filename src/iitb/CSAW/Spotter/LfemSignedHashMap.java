package iitb.CSAW.Spotter;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.io.BinIO;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Smola's suggestion to use signed hash.
 * Lossy compression to compare against Lfem.
 * One instance can be shared across threads for only the
 * {@link #get(int, int, int)} method.
 * 
 * @author soumen
 */
public class LfemSignedHashMap extends ALfeMap {
	private Logger logger = Logger.getLogger(getClass());
	/** We will stop far short of {@link Integer#MAX_VALUE} to avoid troublesome VMs. */
	private static final int maxNativeArraySize = 0x40000000;
	static final String numBucketsKey = LfemSignedHashMap.class.getSimpleName() + ".numBuckets"; 
	static final String bucketsFileName = "Buckets.dat";
	final long nBuckets;
	/** Sequence of bucket arrays to support more than {@link Integer#MAX_VALUE} buckets. */
	static ArrayList<float[]> bob;
	final SignedHashFunction shf;
	/** Space in bytes for three int32s --- leaf, feature, entity */
	public static final int packSize = 3 * (Integer.SIZE / Byte.SIZE); 
	
	public final Monitor collisionMonitor = MonitorFactory.getMonitor("collision", null);
	
	public LfemSignedHashMap(Config conf, long nBuckets) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		super(conf, true);
		if (!lfemBaseDir.isDirectory()) {
			throw new IllegalArgumentException(lfemBaseDir + " is not a directory.");
		}
		this.nBuckets = nBuckets;
		shf = new SignedHashFunction(nBuckets);
		bob = new ArrayList<float[]>();
		long testSum = 0;
		for (int bobx = 0; nBuckets > 0; ++bobx) {
			bob.add(new float[(int) Math.min(maxNativeArraySize, nBuckets)]);
			nBuckets -= bob.get(bobx).length;
			testSum += bob.get(bobx).length;
		}
		assert testSum == this.nBuckets;
		logger.info("Allocated " + this.nBuckets + " buckets over " + bob.size() + " native arrays.");
		collisionMonitor.reset();
	}

	@SuppressWarnings("unchecked")
	public LfemSignedHashMap(Config conf) throws IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		super(conf, false);
		synchronized (getClass()) {
			if (bob == null) {
				bob = (ArrayList<float[]>) BinIO.loadObject(new File(lfemBaseDir, bucketsFileName));
			}			
			long nBuckets = 0;
			for (int bx = 0, bn = bob.size(); bx < bn; ++bx) {
				nBuckets += bob.get(bx).length;
			}
			this.nBuckets = nBuckets;
			logger.info("Loaded " + this.nBuckets + " buckets over " + bob.size() + " native arrays.");
		}
		shf = new SignedHashFunction(nBuckets);
	}
	
	public void store() throws IOException {
		BinIO.storeObject(bob, new File(lfemBaseDir, bucketsFileName));
	}
	
	public void put(int leaf, int feat, int ent, float val) {
		final byte[] out = new byte[LfemSignedHashMap.packSize];
		SignedHashFunction.pack(out, leaf, feat, ent);
		shf.doHash(out);
		final int intBobIndex = (int) (shf.bucket/ maxNativeArraySize);
		final int intArrayIndex = (int) (shf.bucket % maxNativeArraySize);
		final float[] nativeArray = bob.get(intBobIndex);
		/*
		 * Theoretically a slot that is exactly zero need not be empty, but 
		 * the probability is very small.
		 */
		final double oldSlotValue = nativeArray[intArrayIndex];
		collisionMonitor.add(oldSlotValue != 0? 1 : 0);
		nativeArray[intArrayIndex] += val * shf.sign;
	}
	
	public float get(int leaf, int feat, int ent) {
		final byte[] out = new byte[LfemSignedHashMap.packSize];
		SignedHashFunction.pack(out, leaf, feat, ent);
		final int intBobIndex = (int) (shf.bucket / maxNativeArraySize);
		final int intArrayIndex = (int) (shf.bucket % maxNativeArraySize);
		final float[] nativeArray = bob.get(intBobIndex);
		return nativeArray[intArrayIndex] * shf.sign;
	}
}
