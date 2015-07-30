package iitb.CSAW.EntityRank.Variable;

/**
 * Variable indexed by a query ID, an entity, and a snippet. The last 
 * translates to a doc ID and an offset range where the entity mention has
 * been found.
 * @author soumen
 */
public class QueryEntitySnippetVariable extends QueryEntityVariable {
	final int docnum, entBeginOffset, entEndOffset;
	public QueryEntitySnippetVariable(String varBase, String queryId, String entName, int docnum, int entBeginOffset, int entEndOffset) {
		super(varBase, queryId, entName);
		this.docnum = docnum;
		this.entBeginOffset = entBeginOffset;
		this.entEndOffset = entEndOffset;
	}
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof QueryEntitySnippetVariable)) return false;
		QueryEntitySnippetVariable other = (QueryEntitySnippetVariable) obj;
		// note we cannot use super.equals(obj)
		return varBase.equals(other.varBase) && queryId.equals(other.queryId) &&
		entName.equals(other.entName) && docnum == other.docnum &&
		entBeginOffset == other.entBeginOffset && entEndOffset == other.entEndOffset;
	}
	@Override
	public int hashCode() {
		return super.hashCode() ^ docnum ^ entBeginOffset ^ entEndOffset;
	}
	@Override
	public String toString() {
		return super.toString() + ",d=" + docnum + "@" + entBeginOffset + "~" + entEndOffset; 
	}
}
