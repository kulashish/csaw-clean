package iitb.CSAW.Index;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Index.SIP4.Sip4Document;
import iitb.CSAW.Query.TypeBindingQuery;
import it.unimi.dsi.util.Interval;

public class TypeBindingWitness extends AWitness {
	public final int entLongId;
	public final int leaf, rank;
	public final double score;
	
	public TypeBindingWitness(int docId, Interval interval, TypeBindingQuery typeBinding, int entLongId, int leaf, int rank, double score) {
		super(docId, interval, typeBinding);
		this.entLongId = entLongId;
		this.leaf = leaf;
		this.rank = rank;
		this.score = score;
	}
	
	/**
	 * Before {@link Sip4Document} we did not support {@link #leaf}, {@link #rank} and {@link #score}.
	 * @param docId
	 * @param interval
	 * @param typeBinding
	 * @param entLongId
	 */
	public TypeBindingWitness(int docId, Interval interval, TypeBindingQuery typeBinding, int entLongId) {
		this(docId, interval, typeBinding, entLongId, -1, -1, Double.NaN);
	}
	
	@Override
	public String toString() {
		return super.toString() + "=" + entLongId;
	}

	public String toString(ACatalog catalog) {
		return super.toString() + "=" + catalog.entIDToEntName(entLongId);
	}

	@Override
	public double energy() {
		return 0;
	}

	@Override
	public double logEnergy() {
		return 0;
	}
}
