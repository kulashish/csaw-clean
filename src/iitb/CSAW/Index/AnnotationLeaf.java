package iitb.CSAW.Index;

import iitb.CSAW.Spotter.Spot;
import it.unimi.dsi.util.Interval;

/**
 * {@link Annotation} does not include the leaf of the {@link Spot} so we 
 * include it in this subclass to be handed over to indexers.
 * @author soumen
 */
public class AnnotationLeaf extends Annotation {
	private static final long serialVersionUID = 3L;
	public final int leaf;

	public AnnotationLeaf(String entName, Interval span, float score, int rank, int leaf) {
		super(entName, span, score, rank);
		this.leaf = leaf;
	}
}
