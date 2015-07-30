package iitb.CSAW.Query;

import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;

public class RootQuery {
	public final String queryId;
	
	public RootQuery(String queryId) {
		this.queryId = queryId;
	}
	
	public final ReferenceArrayList<ContextQuery> contexts = new ReferenceArrayList<ContextQuery>();
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(qid=" + queryId + " contexts=" + contexts + ")";
	}

	/**
	 * Shallow test of equality based on {@link #queryId}.
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof RootQuery)) {
			return false;
		}
		RootQuery other = (RootQuery) obj;
		return queryId.equals(other.queryId);
	}

	@Override
	public int hashCode() {
		return queryId.hashCode();
	}

	/**
	 * Traverses the query AST in an arbitrary order and visits all {@link MatcherQuery}s.
	 * @param visitor
	 */
	public void visit(IQueryVisitor visitor) {
		for (ContextQuery context : contexts) {
			for (MatcherQuery matcher : context.matchers) {
				visit(matcher, visitor);
			}
		}
	}
	
	private void visit(MatcherQuery matcher, IQueryVisitor visitor) {
		if (matcher instanceof PhraseQuery) {
			for (AtomQuery atom : ((PhraseQuery) matcher).atoms) {
				visit(atom, visitor);
			}
		}
		else {
			visitor.visit(matcher);
		}
	}

	public void removeDocumentState() {
		for (ContextQuery cq : contexts) {
			cq.removeDocumentState();
		}
	}
	
	/**
	 * @return sum of IDF of all token literals in the subtree
	 */
	@Deprecated
	public double sumTokenIdf() {
		double ans = 0;
		for (ContextQuery context : contexts) {
			for (MatcherQuery matcher : context.matchers) {
				if (matcher instanceof TokenLiteralQuery || matcher instanceof PhraseQuery) {
					ans += matcher.sumTokenIdf();
				}
			}
		}
		return ans;
	}
	
	/**
	 * @param stem
	 * @return the first available token literal in the subtree 
	 * whose stem equals {@code stem}.
	 */
	@Deprecated
	public TokenLiteralQuery findLiteral(MutableString stem) {
		for (ContextQuery context : contexts) {
			for (MatcherQuery matcher : context.matchers) {
				final TokenLiteralQuery ans = matcher.findLiteral(stem);
				if (ans != null) {
					return ans;
				}
			}
		}
		return null;
	}
}
