package iitb.CSAW.Query;

import iitb.CSAW.Index.SIP2.Sip2IndexReader.SipSpanIterator;
import it.unimi.dsi.lang.MutableString;

public class EntityLiteralQuery extends AtomQuery implements IQuery {
	public final String entName;

	public EntityLiteralQuery(String entName, Exist exist) {
		super(exist);
		this.entName = entName;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName()+ "(" + exist + ", " + entName + ")";
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof EntityLiteralQuery)) return false;
		EntityLiteralQuery cel = (EntityLiteralQuery) obj;
		return entName.equals(cel.entName);
	}

	@Override
	public int hashCode() {
		return entName.hashCode();
	}
	
	/* Decorations */
	
	public int entId; // from catalog
	public long corpusFreq, nSlots, documentFreq, nDocs;
	
	public void decorate(int entId, long corpusFreq, long nSlots, long documentFreq, long nDocs) {
		this.entId = entId;
		this.corpusFreq = corpusFreq;
		this.nSlots = nSlots;
		this.documentFreq = documentFreq;
		this.nDocs = nDocs;
		if (nSlots <= 0 || nDocs <= 0) {
			throw new IllegalArgumentException(this + ": illegal value nSlots=" + nSlots + " or nDocs=" + nDocs);
		}
	}
	
	/** Index probe support */
//	public IndexIterator indexIterator;
	public SipSpanIterator sipIterator;

	@Override
	public TokenLiteralQuery findLiteral(MutableString stem) {
		return null;
	}

	@Override
	public double sumTokenIdf() {
		throw new UnsupportedOperationException(getClass().getName() + " does not support token IDF");
	}

	@Override
	public double energy() {
		return (double) nDocs / (double) documentFreq;
	}
}
