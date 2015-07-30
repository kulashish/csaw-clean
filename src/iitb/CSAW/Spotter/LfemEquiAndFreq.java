package iitb.CSAW.Spotter;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIterator;
import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Random;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

/**
 * Blend skips at some frequent feature and some equispaced skips. 
 * @author soumen
 */
public class LfemEquiAndFreq extends LfemFreq {
	/** Fraction of leaf budget given to equi. */
	static final double fracFreq = 0.5;
	final Random random = new Random();
	
	@Override
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr, int aleaf,
			int leafBudget, IntList refFeats, IntList refBitEnds,
			DoubleList refProbs, IntArrayList outSkipIndices)
	throws Exception {
		if (leafBudget > refFeats.size()) {
			lfemsa.logger.debug("L" + aleaf + " budget=" + leafBudget + " > " + refFeats.size() + " number of features.");
			leafBudget = refFeats.size();
		}
		outSkipIndices.clear();
		if (leafBudget == 0) {
			return;
		}
		/*
		 * If the leaf budget is very low prefer equi! 
		 */
		final int freqBudget = (int) Math.floor(fracFreq * leafBudget);
		assert 0 <= freqBudget && freqBudget <= leafBudget;
		final IntArrayList freqSkipIndices = new IntArrayList();
		allocateInternal(lfemsa, lfemr, aleaf, freqBudget, refFeats, refBitEnds, refProbs, freqSkipIndices);
		freqSkipIndices.size(Math.min(freqSkipIndices.size(), freqBudget));

		/*
		 * Trying dartboard approach for starters.
		 */
		final TIntHashSet skipIndices = new TIntHashSet();
		for (int freqSkipIndex : freqSkipIndices) {
			skipIndices.add(freqSkipIndex);
		}
		while (skipIndices.size() < leafBudget) {
			final int dart = random.nextInt(refFeats.size());
			skipIndices.add(dart);
		}
		for (TIntIterator six = skipIndices.iterator(); six.hasNext(); ) {
			final int skipIndex = six.next();
			outSkipIndices.add(skipIndex);
		}
		Sorting.quickSort(outSkipIndices.elements(), 0, outSkipIndices.size(), new IntComparator() {
			@Override public int compare(int o1, int o2) { return o1 - o2; }
		});
		assert outSkipIndices.size() == leafBudget : "L" + aleaf + " " + outSkipIndices.size() + " != " +  leafBudget;
	}
}
