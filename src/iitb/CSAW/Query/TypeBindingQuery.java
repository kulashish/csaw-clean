package iitb.CSAW.Query;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Index.SIP2.Sip2IndexReader.SipSpanIterator;
import it.unimi.dsi.lang.MutableString;

public class TypeBindingQuery extends AtomQuery implements IQuery {
	public final String entVarName, typeName;

	public TypeBindingQuery(String varName, String typeName) {
		super(Exist.must); // a binding is always compulsory
		this.entVarName = varName;
		this.typeName = typeName;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + entVarName + " in " + typeName + ")";
	}
	
	/* Decorations */
	
	/** To be filled from {@link ACatalog}. */
	public int typeId = -1;
	public long corpusFreq, nSlots, documentFreq, nDocs;
	
	public void decorate(int typeId, long corpusFreq, long nSlots, long documentFreq, long nDocs) {
		this.typeId = typeId;
		this.corpusFreq = corpusFreq;
		this.nSlots = nSlots;
		this.documentFreq = documentFreq;
		this.nDocs = nDocs;
	}
	
	/** Index probe support */
//	public SipIterator sipIterator;
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
		throw new UnsupportedOperationException(getClass().getName() + " does not support energy");
	}
}
