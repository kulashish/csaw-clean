package iitb.CSAW.Spotter;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import iitb.CSAW.Spotter.LeafFeatureEntityMaps.LfemReader;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Use {@link LfemDynProg} for small leaves and {@link LfemEqui} for larger ones.
 * @author soumen
 */
public class LfemDynProgOrEqui implements ILfemInner {
	static final String monKeyUsedDp = "UsedDynProg";
	final Monitor monUsedDp = MonitorFactory.getMonitor(monKeyUsedDp, null);
	static final int MAXDYN = 60000;
	
	final IntArrayList cofss = new IntArrayList(MAXDYN);
	final LfemDynProg lfemDynProg;
	final LfemEqui lfemEqui;
	
	LfemDynProgOrEqui(Config conf, float[] bbuf_, int[] lbuf_) {
		lfemDynProg = new LfemDynProg(conf, bbuf_, lbuf_);
		lfemEqui = new LfemEqui();
	}

	@Override
	public void allocate(LfemSkipAllocator lfemsa, LfemReader lfemr, int aleaf,
			int leafBudget, IntList refFeats, IntList refBitEnds,
			DoubleList refProbs, IntArrayList outSkipIndices)
	throws Exception {
		final long nFeats = refFeats.size(), nBudget = leafBudget;
		if (nFeats * nBudget * nBudget > MAXDYN) {
			lfemEqui.allocate(lfemsa, lfemr, aleaf, leafBudget, refFeats, refBitEnds, refProbs, outSkipIndices);
			monUsedDp.add(0);
		}
		else {
			cofss.clear();
			for (int fx = 0, fn = refFeats.size(); fx < fn; ++fx) {
				cofss.add(fx);
			}
			lfemDynProg.init(refFeats, refBitEnds, refProbs, cofss, leafBudget);
			lfemDynProg.solve();
			outSkipIndices.clear();
			lfemDynProg.getSkipsSorted(outSkipIndices);
			monUsedDp.add(1);
		}
	}
}
