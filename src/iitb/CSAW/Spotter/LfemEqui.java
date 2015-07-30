package iitb.CSAW.Spotter;

import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

import com.jamonapi.MonitorFactory;

public class LfemEqui implements ILfemInner {
	@Override
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr,
			int aleaf, int leafBudget,
			IntList refFeats, IntList refBitEnds, DoubleList refProbs,
			IntArrayList skipIndices)
	throws IOException {
		skipIndices.clear();
		if (leafBudget == 0 || refFeats.isEmpty()) {
			return;
		}
		assert refFeats.size() > 0 && refBitEnds.size() > 0 && refFeats.size() == refBitEnds.size();
		if (leafBudget > refFeats.size()) {
			lfemsa.logger.warn("leaf=" + aleaf + " must reduce budget=" + leafBudget + " feats=" + refFeats.size());
			leafBudget = refFeats.size();
		}
		final int targetGap = refBitEnds.getInt(refBitEnds.size()-1) / leafBudget;
		int prevSkipBitEnd = 0;
		for (int fx = 0, fn = refFeats.size(); fx < fn; ++fx) {
			final int cBitEnd = refBitEnds.getInt(fx);
			// adding an initial skip not required for correctness as of 2011/09/26
			// but helps us use up the budget more reliably
			if (fx == 0 || cBitEnd - prevSkipBitEnd >= targetGap) {
				skipIndices.add(fx);
				prevSkipBitEnd = cBitEnd;
				if (--leafBudget == 0) {
					break;
				}
			}
		}
		MonitorFactory.getMonitor("UnspentBudget", null).add(skipIndices.size() < leafBudget? 1 : 0);
	}
}
