package iitb.CSAW.Spotter;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TLongIntHashMap;
import gnu.trove.TLongProcedure;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.LongIntInt;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import cern.colt.Sorting;
import cern.colt.function.LongComparator;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * <b>Not thread-safe</b> except when {@link LfemReader} is used.
 * Replacement for {@link LeafEntityFeatureMaps}.
 * 
 * @author soumen
 */
public class LeafFeatureEntityMaps extends ALfeMap {
	/*
	 * Shared between index writing and reading
	 */
	static LongArrayList leafToBitEnd = null;
	final File sampleDir, skipDir, featEntFreqFile, leafToBitEndFile;
	public static final String sampleKeySuffix = ".SampleBase";
	
	/*
	 * For index writing
	 */
	int leaf=Integer.MIN_VALUE;
	TLongIntHashMap featEntToFreqMap;
	final LongArrayList featEntSorter = new LongArrayList();
	OutputBitStream featEntFreqObs;
	/** Makes class thread-unsafe. */
	final LongIntInt liik = new LongIntInt();
	
	/*
	 * For index reading
	 */
	public static class LfemReader {
		final InputBitStream featEntFreqIbs;
		LfemReader(InputBitStream ibs) {
			featEntFreqIbs = ibs;
		}
		public void close() throws IOException {
			featEntFreqIbs.close();
		}
	}
	
	static final String featEntFreqName = "FeatEntFreq.dat";
	static final String leafToBitEndName = "LeafToBitEnd.idx";
	static final String leafToSentEndName = "LeafToSentEnd.dat";
	static final String sentFeatName = "SentFeat.dat";
	static final String sentBitPlusName = "SentBitPlus.dat";
	
	protected static byte[] featEntFreqBuf = null;
	protected static IntArrayList leafToSentEnd = null, sentFeat = null, sentBitPlus = null;

	/** These cosmic constants come from system measurements and are not part of the
	 * <em>policy</em>, so they should not affect paths and properties. */
	final float r0, r1, s0, s1;
	
	final static String usedBudgetKey = ".UsedBudget";
	final static String estTrainCostKey = ".EstimatedTrainCost", estTestCostKey = ".EstimatedTestCost";

	/*
	 * Skip configuration
	 */
	public enum OuterPolicy { Bit, HitBit, SqrtHitBit, NumFeat };
	public enum InnerPolicy { Freq, Equi, EquiAndFreq, DynProgOrEqui };
	public enum ThinPolicy { None, Freq, FreqEqui };
	public enum PermutePolicy { Default, Random, Freq, FreqPerBit };
	
	final OuterPolicy outerPolicy;
	final InnerPolicy innerPolicy;
	final ThinPolicy thinPolicy;
	final PermutePolicy permutePolicy;
	
	final static String globalBudgetKey = ".GlobalBudget";
	final int globalBudget;
	final static String leafSmootherKey = ".LeafSmoother";
	final float leafSmoother;
	
	/** Feature permutations */
	final int[] fwdPerm, revPerm;

	/* Thinning */
	final static String thinBudgetMultipleKey = ".ThinBudgetMultiple";
	final static String thinMaxFeatureKey = ".ThinMaxFeature";
	final static String thinFreqFractionKey = ".ThinFreqFraction";
	final float thinBudgetMultiple;
	/** Actual thinning target is max of {@link #thinMaxFeature} and {@link #thinBudgetMultiple} times leafBudget */
	final int thinMaxFeature;
	/** Fraction of thinned candidates that are from the topk by frequency */
	final float thinFreqFraction;
	
	LeafFeatureEntityMaps(Config conf) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		this(conf, false);
	}

	public LeafFeatureEntityMaps(Config conf, boolean doWrite) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		super(conf, doWrite);
		leafToBitEndFile = new File(lfemBaseDir, leafToBitEndName);
		featEntFreqFile = new File(lfemBaseDir, featEntFreqName);
		final String self = LeafFeatureEntityMaps.class.getCanonicalName();

		// these are constants independent of any policy
		globalBudget = conf.getInt(self + globalBudgetKey);
		r0 = conf.getFloat(PropertyKeys.r0Key);
		r1 = conf.getFloat(PropertyKeys.r1Key);
		s0 = conf.getFloat(PropertyKeys.s0Key);
		s1 = conf.getFloat(PropertyKeys.s1Key);

		outerPolicy = OuterPolicy.valueOf(conf.getString(self + "." + OuterPolicy.class.getSimpleName()));
		innerPolicy = InnerPolicy.valueOf(conf.getString(self + "." + InnerPolicy.class.getSimpleName()));
		thinPolicy = ThinPolicy.valueOf(conf.getString(self + "." + ThinPolicy.class.getSimpleName()));
		permutePolicy = PermutePolicy.valueOf(conf.getString(self + "." + PermutePolicy.class.getSimpleName()));
		leafSmoother = conf.getFloat(self + leafSmootherKey);
		
		final File permFile = new File(lfemBaseDir, permutePolicy.getClass().getCanonicalName() + "." + permutePolicy);
		if (permutePolicy != PermutePolicy.Default && permFile.canRead()) {
			logger.warn("Started loading feature permutation from " + permFile);
			fwdPerm = BinIO.loadInts(permFile);
			revPerm = new int[fwdPerm.length];
			Arrays.fill(revPerm, Integer.MIN_VALUE);
			// check and invert
			for (int preFeat = 0; preFeat < fwdPerm.length; ++preFeat) {
				final int postFeat = fwdPerm[preFeat];
				if (revPerm[postFeat] != Integer.MIN_VALUE) {
					throw new IllegalArgumentException(permFile + " not a permutation.");
				}
				revPerm[postFeat] = preFeat;
			}
			for (int postFeat = 0; postFeat < revPerm.length; ++postFeat) {
				if (revPerm[postFeat] == Integer.MIN_VALUE) {
					throw new IllegalArgumentException(permFile + " not a permutation.");
				}
			}
			logger.info("Finished loading feature permutation from " + permFile);
		}
		else {
			logger.info(permFile + " not readable, no feature permutation.");
			fwdPerm = revPerm = null;
		}
		
		// load up thinning config
		switch (thinPolicy) {
		case None:
			thinBudgetMultiple = thinFreqFraction = Float.NaN;
			thinMaxFeature = Integer.MAX_VALUE;
			break;
		default:
			thinMaxFeature = conf.getInt(self + thinMaxFeatureKey);
			thinBudgetMultiple = conf.getFloat(self + thinBudgetMultipleKey);
			thinFreqFraction = conf.getFloat(self + thinFreqFractionKey);
			break;
		}
		
		sampleDir = new File(lfemBaseDir, conf.getString(self + sampleKeySuffix));
		skipDir = new File(sampleDir, "Skips_" + outerPolicy + "_" + innerPolicy + "_" + globalBudget + "_" + leafSmoother);
	}
	
	void beginIndexing(boolean truncate) throws FileNotFoundException {
		if (truncate) {
			featEntFreqFile.delete();
			leafToBitEndFile.delete();
		}
		else {
			nihilism(featEntFreqFile);
			nihilism(leafToBitEndFile);
		}
		featEntFreqObs = new OutputBitStream(featEntFreqFile);
		leafToBitEnd = new LongArrayList();
		leafToBitEnd.size(trie.getNumLeaves());
		Arrays.fill(leafToBitEnd.elements(), Long.MIN_VALUE);
	}
	
	void beginLeaf(int leaf) {
		if (this.leaf >= leaf) {
			throw new IllegalArgumentException("Leaf did not increase from " + this.leaf + " to " + leaf);
		}
		this.leaf = leaf;
		featEntToFreqMap = new TLongIntHashMap();
		featEntSorter.clear();
		logger.trace("Begin L=" + leaf);
	}
	
	void accumulate(int feat, int ent, int freq) {
		assert leaf >= 0 && ent >= Spot.naEnt;
		liik.write(feat, ent);
		featEntToFreqMap.adjustOrPutValue(liik.lv, freq, freq);
	}
	
	void endLeaf() throws IOException {
		logger.trace("End L=" + leaf);
		featEntSorter.clear();
		featEntToFreqMap.forEachKey(new TLongProcedure() {
			@Override
			public boolean execute(long featEnt) {
				featEntSorter.add(featEnt);
				return true;
			}
		});
		Sorting.quickSort(featEntSorter.elements(), 0, featEntSorter.size(), new LongComparator() {
			@Override
			public int compare(long o1, long o2) {
				liik.write(o1);
				final int feat1 = liik.iv1, ent1 = liik.iv0;
				liik.write(o2);
				final int feat2 = liik.iv1, ent2 = liik.iv0;
				final int cfeat = feat1 - feat2;
				if (cfeat != 0) return cfeat;
				return ent1 - ent2;
			}
		});
		final IntList leafEnts = trie.getSortedEntsNa(leaf);
		// write out leaf block
		int prevFeat = -1, prevShEnt = -1;
		for (long featEnt : featEntSorter) {
			final int freq = featEntToFreqMap.get(featEnt);
			liik.write(featEnt);
			final int feat = liik.iv1, ent = liik.iv0;
			assert feat >= prevFeat : "prevFeat=" + prevFeat + " feat=" + feat;
			final int shEnt = leafEnts.indexOf(ent); // hopefully not too slow
			if (shEnt < 0) {
				throw new IllegalStateException("E" + ent + " not found in " + leafEnts + " for L" + leaf);
			}
			// feat ent shEnt freq
			if (feat > prevFeat) {
				prevShEnt = -1;
			}
			final int fgap = feat - prevFeat; // fgap may be zero
			final int segap = shEnt - prevShEnt - 1; // never negative
			featEntFreqObs.writeGamma(fgap);
			featEntFreqObs.writeGamma(segap); // shortEnt gap gamma --- whew!
			featEntFreqObs.writeGamma(freq);
			prevFeat = feat;
			prevShEnt = shEnt;
		}
		// maintaining side indexes
		leafToBitEnd.set(leaf, featEntFreqObs.writtenBits());
	}
	
	long leafToBitBegin(int aleaf) {
		while (--aleaf >= 0) {
			final long ans = leafToBitEnd.getLong(aleaf);
			if (ans >= 0) {
				return ans;
			}
		}
		return 0;
	}
	
	int leafToSentBegin(int aleaf) {
		while (--aleaf >= 0) {
			final int ans = leafToSentEnd.getInt(aleaf);
			if (ans >= 0) {
				return ans;
			}
		}
		return 0;
	}
	
	void endIndexing() throws IOException {
		featEntFreqObs.close();
		BinIO.storeObject(leafToBitEnd, leafToBitEndFile);
		store();
	}
	
	/**
	 * Fixes bit ends of empty leaves if needed.
	 * @param leafBits completely dense, but has zero for empty leaves;
	 * ignored if null
	 */
	void collectLeafBits(LongList leafBits) {
		logger.info("Collecting leaf bits");
		for (int aleaf = 0, nleaf = leafToBitEnd.size(); aleaf < nleaf; ++aleaf) {
			if (leafToBitEnd.getLong(aleaf) < 0) {
				if (aleaf == 0) {
					leafToBitEnd.set(aleaf, 0);
				}
				else {
					leafToBitEnd.set(aleaf, leafToBitEnd.getLong(aleaf - 1));
				}
			}
		}
		if (leafBits != null) {
			assert leafBits.size() == leafToBitEnd.size();
			Monitor leafBitsMon = MonitorFactory.getMonitor("LeafBits", null);
			for (int aleaf = 0, nl = leafToBitEnd.size(); aleaf < nl; ++aleaf) {
				leafBits.set(aleaf, leafToBitEnd.getLong(aleaf) - (aleaf == 0? 0 : leafToBitEnd.getLong(aleaf-1)));
				leafBitsMon.add(leafBits.getLong(aleaf));
			}
			logger.info(leafBitsMon);
		}
	}
	
	synchronized LfemReader openRead() throws IOException, ClassNotFoundException {
		if (featEntFreqBuf == null || leafToBitEnd == null) {
			featEntFreqBuf = BinIO.loadBytes(featEntFreqFile);
			logger.info("Allocating L-F-E-M buffer once, " + featEntFreqBuf.length + " bytes.");
			leafToBitEnd = (LongArrayList) BinIO.loadObject(leafToBitEndFile);
			collectLeafBits(null);
			gatherLeafBitsStats();
			loadSkipsIfAny();
//			buildEquispacedSkips();
		}
		return new LfemReader(new InputBitStream(featEntFreqBuf));
	}
	
	/**
	 * If {@link #skipDir} is not found or suitable, does not build
	 * <em>any</em> default skips.
	 * @throws ClassNotFoundException 
	 * @throws IOException 
	 */
	private void loadSkipsIfAny() throws IOException, ClassNotFoundException {
		final File leafToSentEndFile = new File(skipDir, leafToSentEndName);
		final File sentFeatFile = new File(skipDir, sentFeatName);
		final File sentBitPlusFile = new File(skipDir, sentBitPlusName);
		if (leafToSentEndFile.canRead() && sentFeatFile.canRead() && sentBitPlusFile.canRead()) {
			logger.info("Loading skip files from " + skipDir);
			leafToSentEnd = (IntArrayList) BinIO.loadObject(leafToSentEndFile);
			sentFeat = (IntArrayList) BinIO.loadObject(sentFeatFile);
			sentBitPlus = (IntArrayList) BinIO.loadObject(sentBitPlusFile);
		}
		else {
			logger.warn("Cannot find skip files in " + skipDir);
			leafToSentEnd = null;
			sentFeat = null;
			sentBitPlus = null;
		}
	}

	/**
	 * For internal debugging only.
	 * @param leaf
	 * @param entFeatToFreq
	 */
	void getLeaf(LfemReader lfemr, int leaf, TLongIntHashMap entFeatToFreq) throws IOException, ClassNotFoundException {
		entFeatToFreq.clear();
		if (leafToBitEnd.getLong(leaf) < 0) return; // no data for this leaf
		final IntList leafEnts = trie.getSortedEntsNa(leaf);
		final long bitBegin = leaf == 0? 0 : leafToBitEnd.getLong(leaf - 1);
		final long availBits = leafToBitEnd.getLong(leaf) - bitBegin;
		lfemr.featEntFreqIbs.position(bitBegin);
		lfemr.featEntFreqIbs.readBits(0);
		int prevFeat = -1, prevShEnt = -1;
		while (lfemr.featEntFreqIbs.readBits() < availBits) {
			final int fgap = lfemr.featEntFreqIbs.readGamma();
			final int segap = lfemr.featEntFreqIbs.readGamma();
			final int freq = lfemr.featEntFreqIbs.readGamma();
			final int feat = prevFeat + fgap;
			if (feat > prevFeat) {
				prevShEnt = -1;
			}
			final int shEnt = prevShEnt + segap + 1;
			final int ent = leafEnts.getInt(shEnt);
			liik.write(ent, feat);
			entFeatToFreq.put(liik.lv, freq);
			prevFeat = feat;
			prevShEnt = shEnt;
		}
		if (lfemr.featEntFreqIbs.readBits() == availBits) {
			logger.debug("L" + leaf + " ok");
		}
		else {
			logger.warn("L" + leaf + " readBits=" + lfemr.featEntFreqIbs.readBits() + " availBits=" + availBits);
		}
	}
	
	private void gatherLeafBitsStats() {
		Monitor leafBitsMon = MonitorFactory.getMonitor("LeafBlockSize", "bits");
		for (int leaf = 0; leaf < leafToBitEnd.size(); ++leaf) {
			final long lEnd = leafToBitEnd.getLong(leaf);
			if (lEnd >= 0) {
				final long lBeg = leaf == 0? 0 : leafToBitEnd.getLong(leaf - 1);
				leafBitsMon.add(lEnd - lBeg);
			}
		}
		logger.info(leafBitsMon);
	}
	
	/**
	 * Does not use sentinels. Reference implementation for checking correctness
	 * and fallback in case of no skips loaded.
	 * @param aleaf
	 * @param afeat
	 * @param entToFreq
	 * @throws IOException
	 */
	void getEntFreqSlow(LfemReader lfemr, int aleaf, int afeat, TIntIntHashMap entToFreq) throws IOException {
		final IntList leafEnts = trie.getSortedEntsNa(aleaf);
		entToFreq.clear();
		final long uptoBit = leafToBitEnd.getLong(aleaf);
		if (uptoBit < 0) return;
		final long fromBit = (aleaf == 0? 0 : leafToBitEnd.getLong(aleaf-1)), availBits = uptoBit - fromBit;
		lfemr.featEntFreqIbs.position(fromBit);
		lfemr.featEntFreqIbs.readBits(0);
		int prevFeat = -1, prevShEnt = -1;
		while (lfemr.featEntFreqIbs.readBits() < availBits) {
			final int fgap = lfemr.featEntFreqIbs.readGamma();
			final int feat = prevFeat + fgap;
			if (feat > prevFeat) {
				prevShEnt = -1;
			}
			final int segap = lfemr.featEntFreqIbs.readGamma();
			final int shEnt = prevShEnt + segap + 1;
			final int ent = leafEnts.getInt(shEnt);
			final int freq = lfemr.featEntFreqIbs.readGamma();
			if (feat > afeat) {
				break;
			}
			if (feat == afeat) {
				entToFreq.adjustOrPutValue(ent, freq, freq);
			}
			prevFeat = feat;
			prevShEnt = shEnt;
		}
	}
	
	/**
	 * Does not assume at least one skip per leaf at the first feature in the leaf block.
	 * @param lfemr for thread-safe access
	 * @param aleaf
	 * @param afeat
	 * @param entToFreq output, cleared at start
	 * @throws IOException
	 */
	void getEntFreq2(LfemReader lfemr, int aleaf, int afeat, TIntIntHashMap entToFreq) throws IOException {
		if (leafToSentEnd == null) {
			getEntFreqSlow(lfemr, aleaf, afeat, entToFreq);
			return;
		}
		entToFreq.clear();
		final int sxEnd = leafToSentEnd.getInt(aleaf);
		if (sxEnd < 0) {
			return; // leaf not populated 
		}
		final int sxBeg = leafToSentBegin(aleaf);
		final long leafBeginBit = (aleaf == 0? 0 : leafToBitEnd.getLong(aleaf - 1)); // assume patched via collectLeafBits
		final long leafEndBit = leafToBitEnd.getLong(aleaf);
		if (sxBeg == sxEnd) { // leaf not allocated any skips, fall back on full leaf block scan
			getEntFreqRange2(lfemr, aleaf, afeat, -1, leafBeginBit, leafEndBit, entToFreq);
		}
		else { // leaf allocated one or more skips, but may not be at first feature in leaf block
			final int sxFind = Arrays.binarySearch(sentFeat.elements(), sxBeg, sxEnd, afeat); // read API spec carefully
			if (sxBeg <= sxFind && sxFind < sxEnd) { // direct hit on skip at afeat
				assert sentFeat.getInt(sxFind) == afeat;
				final long fromBit = leafBeginBit + sentBitPlus.getInt(sxFind);
				final long uptoBit = (sxFind == sxEnd-1)? leafEndBit : (leafBeginBit + sentBitPlus.getInt(sxFind+1));
				getEntFreqRange2(lfemr, aleaf, afeat, afeat, fromBit, uptoBit, entToFreq);
			}
			else { // may need to explore up to two skip ranges
				final int sxIns = - sxFind - 1;	// find = - ins_point - 1 implies ins_point = - find - 1
				if (sxIns == sxBeg) { // from leaf block beginning up to first skip
					final int uptoFeat = sentFeat.getInt(sxIns);
					assert uptoFeat > afeat;
					final long uptoBit = leafBeginBit + sentBitPlus.getInt(sxIns);
					getEntFreqRange2(lfemr, aleaf, afeat, -1, leafBeginBit, uptoBit, entToFreq);
				}
				else if (sxIns == sxEnd) { // from last skip in leaf to leaf block end
					final int fromFeat = sentFeat.getInt(sxIns - 1);
					assert fromFeat < afeat;
					final long fromBit = leafBeginBit + sentBitPlus.getInt(sxIns - 1);
					getEntFreqRange2(lfemr, aleaf, afeat, fromFeat, fromBit, leafEndBit, entToFreq);
				}
				else {
					final int fromFeat = sentFeat.getInt(sxIns - 1), uptoFeat = sentFeat.getInt(sxIns);
					assert fromFeat < afeat && uptoFeat > afeat;
					final long fromBit = leafBeginBit + sentBitPlus.getInt(sxIns - 1);
					final long uptoBit = leafBeginBit + sentBitPlus.getInt(sxIns);
					getEntFreqRange2(lfemr, aleaf, afeat, fromFeat, fromBit, uptoBit, entToFreq);
				}
			}
		}
	}
	
	private void getEntFreqRange2(LfemReader lfemr, int aleaf, int afeat, int fromFeat, long fromBit, long uptoBit, TIntIntHashMap entToFreq) throws IOException {
		final IntList leafEnts = trie.getSortedEntsNa(aleaf);
		final long availBits = uptoBit - fromBit;
		lfemr.featEntFreqIbs.position(fromBit);
		lfemr.featEntFreqIbs.readBits(0);
		int prevFeat = -1, prevShEnt = -1;
		while (lfemr.featEntFreqIbs.readBits() < availBits) {
			final int fgap = lfemr.featEntFreqIbs.readGamma();
			// begin tricky footwork
			final int feat;
			if (prevFeat == -1) {
				if (fromFeat == -1) {
					feat = prevFeat + fgap;
				}
				else {
					feat = fromFeat;
				}
			}
			else {
				feat = prevFeat + fgap;
			}
			// end tricky footwork
			if (feat > prevFeat) {
				prevShEnt = -1;
			}
			final int segap = lfemr.featEntFreqIbs.readGamma();
			final int shEnt = prevShEnt + segap + 1;
			final int ent = leafEnts.getInt(shEnt);
			final int freq = lfemr.featEntFreqIbs.readGamma();
			if (feat > afeat) {
				break;
			}
			if (feat == afeat) {
				entToFreq.adjustOrPutValue(ent, freq, freq);
			}
			prevFeat = feat;
			prevShEnt = shEnt;
		}
	}

	private void nihilism(File file) {
		if (file.exists()) {
			throw new IllegalStateException(file + " should not exist");
		}
	}
	
	/**
	 * Test harness for probing data.
	 * @param args [0..1] config, log; [2]=leaf [3]=feat [4]=ent
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final int leaf = Integer.parseInt(args[2]), feat = Integer.parseInt(args[3]), ent = Integer.parseInt(args[4]);
		AStripeManager.construct(conf); // map HostName
		final LeafFeatureEntityMaps lfem = new LeafFeatureEntityMaps(conf);
		LfemReader lfemr = lfem.openRead();
		final TIntIntHashMap entToFreq = new TIntIntHashMap();
		lfem.getEntFreq2(lfemr, leaf, feat, entToFreq);
		System.out.println(lfem.getClass().getSimpleName() + " L" + leaf + " F" + feat + " E" + ent + " --> " + entToFreq.get(ent));
		lfemr.close();
	}
}
