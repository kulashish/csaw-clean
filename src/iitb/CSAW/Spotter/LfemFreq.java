package iitb.CSAW.Spotter;

import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

import com.jamonapi.MonitorFactory;

public class LfemFreq implements ILfemInner {
	@Override
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr,
			int aleaf, int leafBudget,
			IntList refFeats, IntList refBitEnds, final DoubleList refProbs,
			IntArrayList outSkipIndices)
	throws Exception {
		allocateInternal(lfemsa, lfemr, aleaf, leafBudget, refFeats, refBitEnds, refProbs, outSkipIndices);
		outSkipIndices.size(Math.min(outSkipIndices.size(), leafBudget));
		Arrays.sort(outSkipIndices.elements(), 0, outSkipIndices.size());
		MonitorFactory.getMonitor("UnspentBudget", null).add(outSkipIndices.size() < leafBudget? 1 : 0);
	}
	
	protected void allocateInternal(LfemSkipAllocator lfemsa, LfemReader lfemr,
			int aleaf, int leafBudget,
			IntList refFeats, IntList refBitEnds, final DoubleList refProbs,
			IntArrayList outSkipIndices)
	throws Exception {
		assert refFeats.size() == refBitEnds.size();
		outSkipIndices.clear();
		if (refFeats.isEmpty()) {
			lfemsa.logger.debug("L" + aleaf + " has no features, omitting");
			return;
		}
		for (int fx = 0, fn = refFeats.size(); fx < fn; ++fx) {
			outSkipIndices.add(fx);
		}
		Sorting.quickSort(outSkipIndices.elements(), 0, outSkipIndices.size(), new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final double freq1 = refProbs.getDouble(o1);
				final double freq2 = refProbs.getDouble(o2);
				if (freq1 > freq2) return -1;
				if (freq1 < freq2) return 1;
				return 0;
			}
		});
	}
}
