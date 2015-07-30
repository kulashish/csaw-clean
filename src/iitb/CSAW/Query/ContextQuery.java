package iitb.CSAW.Query;

import iitb.CSAW.Index.AWitness;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

public class ContextQuery implements IQuery {
	public enum Window { ordered, unordered };
	
	public final String contextVarName;
	public final Window window;
	public final int width; // aka slop
	/** If {@link #window} is {@link Window#unordered} we ignore the order of {@link #matchers}. */
	public final ReferenceArrayList<MatcherQuery> matchers = new ReferenceArrayList<MatcherQuery>();

	public ContextQuery(String contextVarName, Window window, int width) {
		super();
		this.contextVarName = contextVarName;
		this.window = window;
		this.width = width;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + contextVarName + "," + window + "," + width + " matchers=" + matchers + ")";
	}
	
	/**
	 * Mutable, not thread-safe!
	 */
	public final ReferenceArrayList<AWitness> witnesses = new ReferenceArrayList<AWitness>();

	public void removeDocumentState() {
		witnesses.clear();
		for (MatcherQuery mq : matchers) {
			mq.removeDocumentState();
		}
	}

	/**
	 * @return
	 * The total energy in all non-binding (i.e., literal) matchers in this 
	 * query subtree. 
	 */
	public double energy() {
		double ans = 0;
		for (MatcherQuery mq : matchers) {
			if (mq instanceof TypeBindingQuery) {
				continue;
			}
			ans += mq.energy();
		}
		return ans;
	}
}
