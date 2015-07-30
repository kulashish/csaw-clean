package iitb.CSAW.Index;

import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import it.unimi.dsi.util.Interval;

public class TokenLiteralWitness extends AWitness {
	public TokenLiteralWitness(int docId, Interval interval, TokenLiteralQuery tokenLiteral) {
		super(docId, interval, tokenLiteral);
	}

	@Override
	public double energy() {
		final TokenLiteralQuery tlq = (TokenLiteralQuery) queryNode;
		if (tlq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get energy of NOT match");
		}
		return 1d * tlq.nDocs / tlq.documentFreq;
	}

	public double logEnergy() {
		final TokenLiteralQuery tlq = (TokenLiteralQuery) queryNode;
		if (tlq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get log energy of NOT match");
		}
		return Math.log(1 + 1d * tlq.nDocs / tlq.documentFreq);
	}
}
