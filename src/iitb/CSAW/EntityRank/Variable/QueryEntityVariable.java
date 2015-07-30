package iitb.CSAW.EntityRank.Variable;

/**
 * Variable indexed by a query ID and an entity (name/ID).
 * @author soumen
 */
public class QueryEntityVariable extends BaseVariable {
	final String queryId, entName;
	public QueryEntityVariable(String varBase, String queryId, String entName) {
		super(varBase);
		this.queryId = queryId;
		this.entName = entName;
	}
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof QueryEntityVariable)) return false;
		QueryEntityVariable other = (QueryEntityVariable) obj;
		// note we cannot use super.equals(other)
		return varBase.equals(other.varBase) && queryId.equals(other.queryId) && entName.equals(other.entName);
	}
	@Override
	public int hashCode() {
		return super.hashCode() ^ queryId.hashCode() ^ entName.hashCode();
	}
	@Override
	public String toString() {
		return super.toString() + "_q=" + queryId + ",e=" + entName;
	}
}
