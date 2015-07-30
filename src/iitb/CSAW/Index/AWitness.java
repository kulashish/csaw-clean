package iitb.CSAW.Index;

import iitb.CSAW.Query.IQuery;
import it.unimi.dsi.util.Interval;

public abstract class AWitness implements Comparable<AWitness> {
	public final int docId;
	public final Interval interval;
	public final IQuery queryNode;

	protected AWitness(int docId, Interval interval, IQuery queryNode) {
		this.docId = docId;
		this.interval = interval;
		this.queryNode = queryNode;
	}

	@Override
	public int compareTo(AWitness o2) {
		final int docDiff = docId - o2.docId;
		if (docDiff != 0) return docDiff;
		if (interval.right < o2.interval.left) return -1;
		if (interval.left > o2.interval.right) return 1;
		return 0;
	}

	@Override
	public String toString() {
		return Integer.toString(docId) + interval + "_" + queryNode;
	}
	
	/**
	 * @return The matched energy in this witness.
	 */
	public abstract double energy();
	
	/**
	 * @return The matched energy in this witness (in log).
	 */
	public abstract double logEnergy();
}
