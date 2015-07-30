package iitb.CSAW.Index;

import java.io.Serializable;

import it.unimi.dsi.util.Interval;

public class Annotation implements Comparable<Annotation>, Serializable {
	private static final long serialVersionUID = 2L;
	public final String entName;
	/** The annotation spans from {@link Interval#left} to
	 * {@link Interval#right} <b>both inclusive</b>. */
	public final Interval interval;
	/** Typically a log probability */
	public final float score;
	/** Of all candidate entities at this annotation, {@link #entName} was at this rank, starting at 0 */
	public final int rank;

	public Annotation(String entName, Interval interval, float score, int rank) {
		this.entName = entName;
		this.interval = interval;
		this.score = score;
		this.rank = rank;
	}
	
	@Override
	public String toString() {
		return interval + entName;
	}

	@Override
	public int compareTo(Annotation o) {
		if (o.interval.left > interval.right) return -1;
		if (o.interval.right < interval.left) return 1;
		return interval.left - o.interval.left;
	}
	
	public static boolean disjoint(Interval span1, Interval span2) {
		return span1.right < span2.left || span2.right < span1.left; 
	}
	
	public static boolean overlaps(Interval span1, Interval span2) {
		return !disjoint(span1, span2);
	}
}
