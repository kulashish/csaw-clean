package iitb.CSAW.Spotter;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntProcedure;
import gnu.trove.TLongIntHashMap;
import gnu.trove.TLongLongHashMap;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.LongIntInt;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;

import org.apache.log4j.Logger;

/**
 * <p><b>Single-thread access only.</b></p>
 * 
 * <p>Stores model data for annotators. Should be reusable across
 * {@link BayesContextBase} and {@link LogisticContextBase}.
 * Logically implements a map from leaf, ent, feature to count or value.</p>
 * 
 * <p>Physically organized as two ganged arrays of gamma compressed integers,
 * one for feature IDs the other for counts/values. There are ganged blocks on
 * these arrays for each (leaf, ent) pair. Although the arrays are implemented
 * as bit streams, each block is byte-aligned. Inside a block, feature IDs are
 * in increasing order. Indices on the side maintain maps from (leaf, ent) to
 * the starting bit and block length in bits for both arrays. For floating
 * point values we will need a different compression scheme.</p>
 * 
 * <p>During training, {@link ContextFeatureVector}s are submitted in
 * increasing (leaf, ent) order, but arbitrary sparse feature orders.
 * We accumulate a feature-to-count map. At the end of the (leaf, ent) block,
 * we sort by feature and write out to two {@link OutputBitStream}s.
 * For features gap gamma is used, for counts, just gamma. We also record
 * the byte offsets as described above. At the end we write out the offset
 * maps to yet more files.</p>
 * 
 * @author soumen
 */
public class LeafEntityFeatureMaps extends ALfeMap {
	static final String lefmBaseDirKey = "Lefm.dir";

	static final String leafEntFeatIdBeginName = "LeafEntFeatIdBegin.idx";
	static final String leafEntFeatIdGammaName = "LeafEntFeatIdGamma.dat";
	static final String leafEntFeatIdSizeName = "LeafEntFeatIdSize.idx";
	static final String leafEntFeatFreqBeginName = "LeafEntFeatFreqBegin.idx";
	static final String leafEntFeatFreqGammaName = "LeafEntFeatFreqGamma.dat";
	static final String leafEntFeatFreqSizeName = "LeafEntFeatFreqSize.idx";

	final Logger logger = Logger.getLogger(getClass());
	
	/*
	 * Shared between writing and reading
	 */
	final File lefmBaseDir;
	final File featureGammaFile, featureBeginFile, featureSizeFile;
	final File countGammaFile, countBeginFile, countSizeFile;
	/* bit based offsets and sizes */
	TLongLongHashMap leafEntToFeatureBegin, leafEntToCountBegin;
	TLongIntHashMap leafEntToFeatureSize, leafEntToCountSize;
	
	/*
	 * For index writing
	 */
	int leaf=Integer.MIN_VALUE, ent=Integer.MIN_VALUE;
	TIntIntHashMap featureToCountMap = new TIntIntHashMap();
	final IntArrayList featureSorter = new IntArrayList();
	final LongIntInt leafEnt = new LongIntInt();
	OutputBitStream featureObs, countObs;
	
	/*
	 * For index reading
	 */
	InputBitStream featureIbs, countIbs;

	LeafEntityFeatureMaps(Config conf) throws Exception {
		this(conf, false);
	}
	
	LeafEntityFeatureMaps(Config conf, boolean doWrite) throws Exception {
		super(conf, doWrite);
		lefmBaseDir = new File(conf.getString(lefmBaseDirKey));
		featureGammaFile = new File(lefmBaseDir, leafEntFeatIdGammaName);
		featureBeginFile = new File(lefmBaseDir, leafEntFeatIdBeginName);
		featureSizeFile = new File(lefmBaseDir, leafEntFeatIdSizeName);
		countGammaFile = new File(lefmBaseDir, leafEntFeatFreqGammaName);
		countBeginFile = new File(lefmBaseDir, leafEntFeatFreqBeginName);
		countSizeFile = new File(lefmBaseDir, leafEntFeatFreqSizeName);
	}
	
	public File getBaseDir() { return lefmBaseDir; }
	
	private void nihilism(File file) {
		if (file.exists()) {
			throw new IllegalStateException(file + " should not exist");
		}
	}
	
	void beginIndexing(boolean truncate) throws FileNotFoundException {
		if (truncate) {
			featureGammaFile.delete();
			featureBeginFile.delete();
			featureSizeFile.delete();
			countGammaFile.delete();
			countBeginFile.delete();
			countSizeFile.delete();
		}
		else {
			nihilism(featureGammaFile);
			nihilism(featureBeginFile);
			nihilism(featureSizeFile);
			nihilism(countGammaFile);
			nihilism(countBeginFile);
			nihilism(countSizeFile);
		}
		featureObs = new OutputBitStream(featureGammaFile);
		countObs = new OutputBitStream(countGammaFile);
		
		leafEntToFeatureBegin = new TLongLongHashMap();
		leafEntToCountBegin = new TLongLongHashMap();
		leafEntToFeatureSize = new TLongIntHashMap();
		leafEntToCountSize = new TLongIntHashMap();
	}
	
	void beginLeafEntBlock(int leaf, int ent) {
		if (this.leaf > leaf) {
			throw new IllegalArgumentException("Leaf must not decrease from " + this.leaf + " to " + leaf);
		}
		if (this.leaf == leaf && this.ent > ent) {
			throw new IllegalArgumentException("Ent must not decrease from " + this.ent + " to " + ent);
		}
		this.leaf = leaf;
		this.ent = ent;
		featureToCountMap = new TIntIntHashMap();
		featureSorter.clear();
		logger.trace("Begin L=" + leaf + " E=" + ent);
	}
	
	void accumulate(int feature, int count) {
		assert feature >= 0 : "Negative feature " + feature;
		featureToCountMap.adjustOrPutValue(feature, count, count);
	}
	
	void endLeafEntBlock() throws IOException {
		logger.trace("End L=" + leaf + " E=" + ent + " features=" + featureToCountMap.size());

		featureSorter.clear();
		featureToCountMap.forEachKey(new TIntProcedure() {
			@Override
			public boolean execute(int feature) {
				assert feature >= 0 : "Negative feature " + feature;
				featureSorter.add(feature);
				return true;
			}
		});
		Collections.sort(featureSorter);
		
		// make sure we start at a byte boundary
		featureObs.flush();
		countObs.flush();
		leafEnt.write(leaf, ent);
		assert featureObs.writtenBits() % Byte.SIZE == 0;
		final long leafEntFeatureBegin = featureObs.writtenBits();
		leafEntToFeatureBegin.put(leafEnt.lv, leafEntFeatureBegin);
		assert countObs.writtenBits() % Byte.SIZE == 0;
		final long leafEntCountBegin = countObs.writtenBits();
		leafEntToCountBegin.put(leafEnt.lv, leafEntCountBegin);
		
		int prevFeature = -1; // because the first real feature could be 0
		for (int feature : featureSorter) {
			final int fgap = feature - prevFeature - 1;
			assert fgap >= 0 : "Negative feature gap " + feature + ", " + prevFeature;
			final int count = featureToCountMap.get(feature);
			featureObs.writeGamma(fgap);
			countObs.writeGamma(count);
			logger.trace(leaf + "\t" + ent + "\t" + feature + "\t" + count);
			prevFeature = feature;
		}
		
		final long leafEntFeatureEnd = featureObs.writtenBits();
		final long leafEntFeatureSize = leafEntFeatureEnd - leafEntFeatureBegin;
		assert leafEntFeatureSize <= Integer.MAX_VALUE;
		leafEntToFeatureSize.put(leafEnt.lv, (int) leafEntFeatureSize);
		
		final long leafEntCountEnd = countObs.writtenBits();
		final long leafEntCountSize = leafEntCountEnd - leafEntCountBegin;
		assert leafEntCountSize <= Integer.MAX_VALUE;
		leafEntToCountSize.put(leafEnt.lv, (int) leafEntCountSize);
		
		featureToCountMap = null;
		featureSorter.clear();
	}
	
	void endIndexing() throws IOException {
		logger.info("Closing index with featureBytes=" + featureGammaFile.length() + "," + featureObs.writtenBits()/Byte.SIZE);
		logger.info("Closing index with countBytes=" + countGammaFile.length() + "," + countObs.writtenBits()/Byte.SIZE);
		featureObs.close();
		countObs.close();
		BinIO.storeObject(leafEntToFeatureBegin, featureBeginFile);
		BinIO.storeObject(leafEntToFeatureSize, featureSizeFile);
		BinIO.storeObject(leafEntToCountBegin, countBeginFile);
		BinIO.storeObject(leafEntToCountSize, countSizeFile);
	}
	
	void openRead() throws IOException, ClassNotFoundException {
		leafEntToFeatureBegin = (TLongLongHashMap) BinIO.loadObject(featureBeginFile);
		leafEntToFeatureSize = (TLongIntHashMap) BinIO.loadObject(featureSizeFile);
		leafEntToCountBegin = (TLongLongHashMap) BinIO.loadObject(countBeginFile);
		leafEntToCountSize = (TLongIntHashMap) BinIO.loadObject(countSizeFile);
		featureIbs = new InputBitStream(BinIO.loadBytes(featureGammaFile));
		countIbs = new InputBitStream(BinIO.loadBytes(countGammaFile));
	}
	
	int getNumPosts(int leaf, int ent) throws IOException {
		leafEnt.write(leaf, ent);
		final long fbeg = leafEntToFeatureBegin.get(leafEnt.lv);
		final long flen = leafEntToFeatureSize.get(leafEnt.lv);
		final long cbeg = leafEntToCountBegin.get(leafEnt.lv);
		final long clen = leafEntToCountSize.get(leafEnt.lv);
		logger.debug("L" + leaf + " E" + ent + " fid=" + fbeg + "+" + flen + " frq=" + cbeg + "+" + clen);
		featureIbs.flush();
		featureIbs.readBits(0);
		featureIbs.position(fbeg);
		countIbs.flush();
		countIbs.readBits(0);
		countIbs.position(cbeg);
		int numPosts = 0;
		int prevFeat = -1;
		while (featureIbs.readBits() < flen && countIbs.readBits() < clen) {
			final int fgap = featureIbs.readGamma();
			final int feat = prevFeat + fgap + 1;
			final int count = countIbs.readGamma();
			logger.trace(leaf + "\t" + ent + "\t" + feat + "\t" + count);
			prevFeat = feat;
			++numPosts;
		}
		return numPosts;
	}
	
	/**
	 * Inefficient because we start at the beginning of the (leaf,ent) block and
	 * scan through gap gamma encoded features until we find the one we want.
	 * @param leaf
	 * @param ent
	 * @param feature
	 * @return
	 * @throws IOException 
	 */
	int getCount(int leaf, int ent, int feature) throws IOException {
		leafEnt.write(leaf, ent);
		final long fbeg = leafEntToFeatureBegin.get(leafEnt.lv);
		final long flen = leafEntToFeatureSize.get(leafEnt.lv);
		final long cbeg = leafEntToCountBegin.get(leafEnt.lv);
		final long clen = leafEntToCountSize.get(leafEnt.lv);
		featureIbs.flush();
		featureIbs.readBits(0);
		featureIbs.position(fbeg);
		countIbs.flush();
		countIbs.readBits(0);
		countIbs.position(cbeg);
		int prevFeat = -1;
		while (featureIbs.readBits() < flen && countIbs.readBits() < clen) {
			final int fgap = featureIbs.readGamma();
			final int feat = prevFeat + fgap + 1;
			final int count = countIbs.readGamma();
			if (feature == feat) {
				return count;
			}
			prevFeat = feat;
		}
		return 0; // feature not found
	}
	
	void closeRead() throws IOException {
		leafEntToFeatureBegin = null;
		leafEntToFeatureSize = null;
		leafEntToCountBegin = null;
		leafEntToCountSize = null;
		featureIbs.close();
		countIbs.close();
	}
	
	/**
	 * Test harness for probing data.
	 * @param args [0..1] config, log; [2]=leaf [3]=ent [4]=feat
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final int leaf = Integer.parseInt(args[2]), ent = Integer.parseInt(args[3]), feat = Integer.parseInt(args[4]);
		final LeafEntityFeatureMaps lefm = new LeafEntityFeatureMaps(conf);
		lefm.openRead();
		System.out.println(lefm.getClass().getSimpleName() + " L" + leaf + " E" + ent + " F" + feat + " --> " + lefm.getCount(leaf, ent, feat));
		lefm.closeRead();
	}
}
