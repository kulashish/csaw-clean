package iitb.CSAW.Query;

import iitb.CSAW.Index.AWitness;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;

public abstract class MatcherQuery implements IQuery {
	public enum Exist { must, may, not };
	public final String[] existStrings = { "+", "", "-" };
	public final Exist exist;
	
	/* Query time attachments -- not thread safe */
	public final ReferenceArrayList<AWitness> witnesses = new ReferenceArrayList<AWitness>();
	
	public MatcherQuery(Exist exist) {
		this.exist = exist;
	}

	public void removeDocumentState() {
		witnesses.clear();
	}
	
	public abstract double energy();

	abstract double sumTokenIdf();
	abstract TokenLiteralQuery findLiteral(MutableString stem);
}
