package iitb.CSAW.Index;

import iitb.CSAW.Index.SIP4.Sip4Document;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import it.unimi.dsi.util.Interval;

public class EntityLiteralWitness extends AWitness {
	public final int leaf, rank;
	public final double score;
	
	public EntityLiteralWitness(int docId, Interval interval, EntityLiteralQuery entityLiteral, int leaf, int rank, double score) {
		super(docId, interval, entityLiteral);
		this.leaf = leaf;
		this.rank = rank;
		this.score = score;
	}
	
	/**
	 * Before {@link Sip4Document} we did not support {@link #leaf}, {@link #rank} and {@link #score}.
	 * @param docId
	 * @param interval
	 * @param entityLiteral
	 */
	public EntityLiteralWitness(int docId, Interval interval, EntityLiteralQuery entityLiteral) {
		this (docId, interval, entityLiteral, -1, -1, Double.NaN);
	}

	/**
	 * Currently defined as simple IDF (no logarithm)
	 */
	@Override
	public double energy() {
		EntityLiteralQuery elq = (EntityLiteralQuery) queryNode;
		if (elq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get energy of NOT match");
		}
		return 1d * elq.nDocs / elq.documentFreq;
	}

	public double logEnergy() {
		EntityLiteralQuery elq = (EntityLiteralQuery) queryNode;
		if (elq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get log energy of NOT match");
		}
		return Math.log(1 + 1d * elq.nDocs / elq.documentFreq);
	}
}
