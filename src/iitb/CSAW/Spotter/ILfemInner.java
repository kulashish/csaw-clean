package iitb.CSAW.Spotter;

import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

public interface ILfemInner {
	/**
	 * Note that these do not write {@link LeafFeatureEntityMaps#leafToSentEnd},
	 * {@link LeafFeatureEntityMaps#sentBitPlus} or
	 * {@link LeafFeatureEntityMaps#sentFeat} arrays.
	 * 
	 * @param lfemsa access to several "parent" fields
	 * @param lfemr for multithreaded access to the LFEM buffer
	 * @param aleaf
	 * @param leafBudget
	 * @param refFeats reference feature list, sorted
	 * @param refProbs snapped reference feature probabilities ganged to refFeats
	 * @param outSkipIndices indices into refFeats <i>in increasing order</i>
	 * @throws Exception
	 */
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr,
			int aleaf, int leafBudget,
			IntList refFeats, IntList refBitEnds, DoubleList refProbs,
			IntArrayList outSkipIndices)
	throws Exception;
}
