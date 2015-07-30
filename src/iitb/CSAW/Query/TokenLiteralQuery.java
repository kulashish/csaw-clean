package iitb.CSAW.Query;

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.IndexIterator;

public class TokenLiteralQuery extends AtomQuery implements IQuery {
	public final String tokenText;

	/* Decorations */
	public final MutableString tokenStem = new MutableString();
	public long corpusFreq, nSlots, documentFreq, nDocs;
	
	public TokenLiteralQuery(String tokenText, Exist exist) {
		super(exist);
		this.tokenText = tokenText;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+ "(" + existStrings[exist.ordinal()] + tokenText + ")";
	}
	
	public void decorate(MutableString tokenStem, long corpusFreq, long nSlots, long documentFreq, long nDocs) {
		this.tokenStem.replace(tokenStem);
		this.corpusFreq = corpusFreq;
		this.nSlots = nSlots;
		this.documentFreq = documentFreq;
		this.nDocs = nDocs;
		if (nSlots <= 0 || nDocs <= 0) {
			throw new IllegalArgumentException(this + ": illegal value nSlots=" + nSlots + " or nDocs=" + nDocs);
		}
	}
	
	/** Index probe support */
	public IndexIterator indexIterator;
	
	@Override
	public double energy() {
		return (double) nDocs / (double) documentFreq;
	}

	@Override
	public double sumTokenIdf() {
		return (double) nDocs / (double) documentFreq;
	}

	@Override
	public TokenLiteralQuery findLiteral(MutableString stem) {
		return tokenStem.equals(stem)? this : null;
	}

}
