package iitb.CSAW.Spotter;

import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.logging.ProgressLogger;

import java.util.Arrays;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

public class LfemDynProg implements ILfemInner {
	/*
	 * Reused across leaf DP problems.
	 */
	/** Cost model parameters */
	final float r0, r1, s0, s1;
	final static float eps = 1e-3f;
	final Logger logger = Logger.getLogger(getClass());
	final float[] bbuf_;
	final int[] lbuf_;
	final IntList cofss = new IntArrayList(), feats = new IntArrayList();
	final DoubleList probs = new DoubleArrayList();
	final FloatList probPrefix = new FloatArrayList(), bitProbPrefix = new FloatArrayList();
	/** Within a leaf we assume at most {@link Integer#MAX_VALUE} bits */
	final IntList bitEnds = new IntArrayList();
	
	/** Reinitialized for each leaf DP problem. */
	int un, cn, kn;
	
	/**
	 * @param conf
	 */
	LfemDynProg(Config conf, float[] bbuf_, int[] lbuf_) {
		r0 = conf.getFloat(PropertyKeys.r0Key);
		r1 = conf.getFloat(PropertyKeys.r1Key);
		s0 = conf.getFloat(PropertyKeys.s0Key);
		s1 = conf.getFloat(PropertyKeys.s1Key);
		this.bbuf_ = bbuf_;
		this.lbuf_ = lbuf_;
		un = cn = kn = Integer.MIN_VALUE;
	}

	/**
	 * Must call this before {@link #solve()};
	 * @param feats_ feature IDs in increasing order
	 * @param bitEnds_ ganged with refFeats
	 * @param probs_ ganged with refFeats
	 * @param cofss_ candidate offsets ganged with feats. Element should be negative if not candidate.
	 * @param kn_ skip budget
	 */
	void init(IntList feats_, IntList bitEnds_, DoubleList probs_, IntList cofss_, int kn_) {
		this.feats.clear();
		this.feats.addAll(feats_);
		this.bitEnds.clear();
		this.bitEnds.addAll(bitEnds_);
		this.probs.clear();
		this.probs.addAll(probs_);
		this.cofss.clear();
		this.cofss.addAll(cofss_);
		
		// mutations -- add one fake candidate at end
		final int maxFeat = feats.getInt(feats.size() - 1);
		feats.add(maxFeat + 1);
		final int cnMinus1 = countCandidates(cofss);
		cofss.add(cnMinus1);
		probs.add(0);
		bitEnds.add(bitEnds.getInt(bitEnds.size()-1));

		// check all array sizes
		un = cofss.size();
		assert un == feats.size();
		assert un == probs.size();
		assert un == bitEnds.size();
		cn = cnMinus1 + 1;
		kn = kn_;
		if (bbuf_.length < cn * (kn+1) || lbuf_.length < cn * (kn+1)) {
			throw new IllegalArgumentException("Problem too big: budget=" + kn + " candidates=" + cn + " features " + un);
		}

		assert bbuf_.length >= cn * (kn + 1);
		Arrays.fill(bbuf_, 0, cn * (kn + 1), Float.NaN);
		assert lbuf_.length >= cn * (kn + 1);
		Arrays.fill(lbuf_, 0, cn * (kn + 1), Integer.MIN_VALUE);

		probPrefix.size(feats.size());
		bitProbPrefix.size(feats.size());
		for (int fx = 0, fn = feats.size(); fx < fn; ++fx) {
			final float pUpto = (fx == 0? 0 : probPrefix.getFloat(fx-1));
			probPrefix.set(fx, (float) (pUpto + probs.getDouble(fx)));
			final float bpUpto = (fx == 0? 0 : bitProbPrefix.getFloat(fx-1));
			final float bits = bitEnds.getInt(fx) - (fx == 0? 0 : bitEnds.getInt(fx-1));
			bitProbPrefix.set(fx, bpUpto + (float) ( probs.getDouble(fx) * bits ) );
		}
	}
	
	int countCandidates(IntList cofss) {
		int ans = 0;
		for (int cofs : cofss) {
			if (cofs >= 0) ++ans;
		}
		return ans;
	}

	/**
	 * @param ux the feature in the next slot of {@link #feats} must be a candidate
	 * @param kx
	 * @return
	 */
	float getB(int ux, int kx) {
		assert ux+1 < cofss.size() && cofss.getInt(ux+1) >= 0;
		if (ux < 0) return 0;
		assert 0 <= ux && ux < un && 0 <= kx && kx <= kn : "u=" + ux + " k=" + kx;
		return bbuf_[cofss.getInt(ux+1) * (1+kn) + kx];
	}
	
	float setB(int ux, int kx, float nv) {
		assert ux+1 < cofss.size() && cofss.getInt(ux+1) >= 0;
		assert 0 <= ux && ux < un && 0 <= kx && kx <= kn;
		final float ov = bbuf_[cofss.getInt(ux+1) * (1+kn) + kx];
		bbuf_[cofss.getInt(ux+1) * (1+kn) + kx] = nv;
		return ov;
	}

	int getL(int ux, int kx) {
		assert ux+1 < cofss.size() && cofss.getInt(ux+1) >= 0;
		assert 0 <= ux && ux < un && 0 <= kx && kx <= kn;
		return lbuf_[cofss.getInt(ux+1) * (1+kn) + kx];
	}
	
	int setL(int ux, int kx, int nv) {
		assert ux+1 < cofss.size() && cofss.getInt(ux+1) >= 0;
		assert 0 <= ux && ux < un && 0 <= kx && kx <= kn;
		final int ov = lbuf_[cofss.getInt(ux+1) * (1+kn) + kx];
		lbuf_[cofss.getInt(ux+1) * (1+kn) + kx] = nv;
		return ov;
	}
	
	/**
	 * Must call {@link #init(IntList, IntList, DoubleList, IntList, int)}
	 * before calling this.
	 */
	void solve() {
		ProgressLogger pl = new ProgressLogger(logger);
		pl.priority = Level.DEBUG;
		pl.expectedUpdates = un * (kn + 1);
		pl.start("Starting DP with " + pl.expectedUpdates + " cells");
		
		// the zero budget column
		{
			float cumulativeCost = 0;
			for (int ux = 0; ux < un-1; ++ux) { // last one is fake
				final float uxBitsToScan = ux == 0? 0 : bitEnds.get(ux-1);
				final float uxBitsToRead = bitEnds.getInt(ux) - uxBitsToScan;
				final float uxReadCost = r0 + r1 * uxBitsToRead;
				final float uxScanCost = (uxBitsToScan > 0? s0 + s1 * uxBitsToScan : 0);
				final float uxProb = (float) probs.getDouble(ux);
				final float uxCost = uxProb * (uxReadCost + uxScanCost); 
				cumulativeCost += uxCost;
				if (cofss.getInt(ux+1) < 0) {
					continue;
				}
				setB(ux, 0, cumulativeCost);
				setL(ux, 0, Integer.MIN_VALUE);
				pl.update();
			}
		}
		
		for (int kx = 1; kx <= kn; ++kx) { // for every budget, kn included
			for (int ux = 0; ux < un-1; ++ux) { // for every feature
				if (cofss.getInt(ux+1) < 0) {
					continue;
				}
//				logger.debug("u=" + ux + " k=" + kx);
				float minB = Float.POSITIVE_INFINITY;
				int minL = Integer.MIN_VALUE;
				if (ux >= kx-1 && cofss.getInt(ux) >= 0) {
					// the ell = ux case: B[u − 1, k − 1] + r0 p(u) + r1 p(u)b(u)
					final float recurs = getB(ux-1, kx-1);
					final float r0pu = r0 * (probPrefix.getFloat(ux) - (ux == 0? 0 : probPrefix.getFloat(ux-1)) );
					final float r1pubu = r1 * (bitProbPrefix.getFloat(ux) - (ux == 0? 0 : bitProbPrefix.getFloat(ux-1)));
					final float testB = recurs + r0pu + r1pubu;
					if (minB > testB) {
						minB = testB;
						minL = ux;
					}
				}
				float gee = 0;
				for (int ell = ux-1; ell >= kx-1; --ell) {
					final float pEll1u = probPrefix.getFloat(ux) - (ell < 0? 0 : probPrefix.getFloat(ell));
					gee += pEll1u * (bitEnds.getInt(ell) - (ell == 0? 0 : bitEnds.getInt(ell-1)) );
					if (cofss.getInt(ell) < 0) {
						continue;
					}
					final float recurs = getB(ell-1, kx-1);
					final float r0pEllU = r0 * (probPrefix.getFloat(ux) - (ell <= 0? 0 : probPrefix.getFloat(ell-1)));
					final float r1pbEllU = r1 * (bitProbPrefix.getFloat(ux) - (ell <= 0? 0 : bitProbPrefix.getFloat(ell-1)));
					final float s0pEll1u = s0 * (probPrefix.getFloat(ux) - probPrefix.getFloat(ell));
					final float testB = recurs + r0pEllU + r1pbEllU + s0pEll1u + s1 * gee;
//					logger.trace("\tell=" + ell + " gee=" + gee + " recurs=" + recurs + " testB=" + testB);
					if (minB > testB) {
						minB = testB;
						minL = ell;
					}
				}
				setB(ux, kx, minB);
				setL(ux, kx, minL);
//				logger.debug("\t.... " + minB);
				pl.update();
			}
		}
		pl.stop();
		pl.done();
	}
	
	float objective() {
		return getB(un-2, kn);
	}
	
	/**
	 * @param skipIndices Note that these are not feature IDs, but 
	 * offsets into {@link #feats}.
	 */
	void getSkipsSorted(IntArrayList skipIndices) {
		skipIndices.clear();
		for (int ell = un - 2, kx = kn; ell >= 0 && kx >= 0;) {
			final int pell = getL(ell, kx);
			if (pell >= 0) {
				skipIndices.add(pell);
				ell = pell-1;
				--kx;
			}
			else {
				break;
			}
		}
		Arrays.sort(skipIndices.elements(), 0, skipIndices.size());
	}
	
	void traceBack(Level level) {
		logger.log(level, "Trace back");
		for (int ell = un - 2, kx = kn; ell >= 0 && kx >= 0;) {
			final float cost = getB(ell, kx);
			final int pell = getL(ell, kx);
			if (pell >= 0) {
				logger.log(level, String.format("ell=%d,%d cost=%5g pell=%d,%d", ell, feats.getInt(ell), cost, pell, feats.getInt(pell)));
				ell = pell-1;
				--kx;
			}
			else {
				break;
			}
		}
	}
	
	void debugPrint(Level level) {
		logger.log(level, un + " feats, " + cn + " cands, " + kn + " budget, cofss=" + cofss);
		for (int ux = 0; ux < un-1; ++ux) {
			final int candx = cofss.getInt(ux+1);
			if (candx < 0) {
				continue;
			}
			StringBuilder sb = new StringBuilder();
			sb.append(String.format("%4d %4d %10d %10d %9.3g __ ", ux, candx, feats.getInt(ux), bitEnds.getInt(ux), probs.getDouble(ux)));
			for (int kx = 0; kx <= kn; ++kx) {
				final int pell = getL(ux, kx);
				final float bval = getB(ux, kx);
				sb.append(String.format("%8g,%-8s\t", bval, (pell == Integer.MIN_VALUE? "null" : Integer.toString(pell))));
				if (kx > 0 && !Float.isInfinite(bval)) {
					final float pval =  getB(ux, kx-1);
					/*
					 * Adding a sentinel should not increase cost in the model.
					 * May raise spurious exceptions owing to numerical comparison tolerance. 
					 */
					if (bval > pval + eps * pval) {
						throw new IllegalStateException(String.format("B[%d,%d]=%g B[%d,%d]=%g", ux, kx-1, pval, ux, kx, bval));
					}
				}
			}
			logger.log(level, sb);
		}
		traceBack(level);
	}

	@Override
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr,
			int aleaf, int leafBudget,
			IntList refFeats, IntList refBitEnds, DoubleList refProbs,
			IntArrayList outSkipIndices)
	throws Exception {
		throw new NotImplementedException();
	}
	
	/**
	 * As of 2011/09/21 we only take the most frequent features,
	 * later we might add some random ones.
	 *
	 * @param leafBudget number of skips allocated to current leaf
	 * @param refFeats feature probabilities from payload corpus
	 * @param refProbs canonical order of reference feature set
	 * @param rfToCf ganged to refFeats, {@link Integer#MIN_VALUE} if not chosen,
	 * candidate feature index if chosen
	 * @return number of surviving features that are candidates for skips 
	 */
	static int thinDownCandidateFeatures(LfemSkipAllocator lfemsa, int leafBudget, final IntList refFeats, final DoubleList refProbs, final IntList rfToCf) {
		final int nAfterThin;
		switch (lfemsa.thinPolicy) {
		case None:
		{
			nAfterThin = rfToCf.size();
			for (int cfx = 0; cfx < nAfterThin; ++cfx) {
				rfToCf.set(cfx, cfx); // all features are candidates
			}
			break;
		}
		case Freq:
		{
			for (int cfx = 0, cfn = rfToCf.size(); cfx < cfn; ++cfx) {
				rfToCf.set(cfx, Integer.MIN_VALUE);
			}
			final int[] rfIds = new int[refProbs.size()];
			for (int rix = 0, rin = refProbs.size(); rix < rin; ++rix) {
				rfIds[rix] = rix;
			}
			Sorting.quickSort(rfIds, 0, rfIds.length, new IntComparator() {
				@Override
				public int compare(int o1, int o2) {
					final double diff = refProbs.getDouble(o1) - refProbs.getDouble(o2);
					if (diff < 0) return 1;  // decreasing probability order
					if (diff > 0) return -1;
					return o1-o2;
				}
			});
			final int capLimit = Math.min(LfemSkipAllocator.dynProgMatrixMaxCells / (leafBudget + 1), lfemsa.thinMaxFeature);
			final int decidedCap = Math.max(capLimit, (int)(lfemsa.thinBudgetMultiple * leafBudget));
			final int prefixSize = Math.min(decidedCap , rfIds.length);
			Arrays.sort(rfIds, 0, prefixSize); // the selected features are brought back into sorted order
			for (int rix = 0; rix < prefixSize; ++rix) {
				rfToCf.set(rfIds[rix], rix);
			}
			nAfterThin = prefixSize;
			break;
		}
		case FreqEqui:
		{
			throw new NotImplementedException();
		}
		default:
			throw new IllegalArgumentException();
		}
		return nAfterThin;
	}
}
