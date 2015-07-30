package iitb.CSAW.Query;

public abstract class AtomQuery extends MatcherQuery implements IQuery {
	public AtomQuery(Exist exist) {
		super(exist);
	}
}
