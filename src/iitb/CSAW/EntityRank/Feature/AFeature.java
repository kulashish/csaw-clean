package iitb.CSAW.EntityRank.Feature;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;
import iitb.CSAW.EntityRank.Wikipedia.Snippet;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.util.Interval;

public abstract class AFeature {
	/**
	 * This forces subclasses to implement a standard constructor.
	 * @param config
	 */
	public AFeature(Config config) { }
	
	@Deprecated
	public abstract double value(RootQuery query, Snippet snippet);
	
	public abstract double value2(ContextWitness cw);
	
	/**
	 * For getting values defining the feature. We can use it to define more granular features 
	 * @param cw
	 * @param positions
	 * @param values
	 * @return
	 */
	public abstract double value3(ContextWitness cw, DoubleArrayList positions, DoubleArrayList values);
	
	
	boolean areDisjoint(Interval int1, Interval int2) {
		return int1.right < int2.left || int2.right < int1.left;
	}
	
	/**
	 * For starters this will be the reciprocal of the shortest token gap
	 * between the two intervals. If the intervals overlap, 1 will be returned.
	 * @param tbw
	 * @param mw
	 * @return
	 */
	double proximity(AWitness tbw, AWitness mw) {
		if (!areDisjoint(tbw.interval, mw.interval)) {
			return 1;
		}
		if (tbw.interval.right < mw.interval.left) {
			final double dist = mw.interval.left - tbw.interval.right;
			return 1d / dist;
		}
		else if (mw.interval.right < tbw.interval.left) {
			final double dist = tbw.interval.left - mw.interval.right;
			return 1d / dist;
		}
		throw new IllegalArgumentException("cannot compute proximity between " + tbw + " and " + mw);
	}
}
