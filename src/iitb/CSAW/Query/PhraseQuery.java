package iitb.CSAW.Query;

import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;

public class PhraseQuery extends MatcherQuery implements IQuery {
	public final ReferenceArrayList<AtomQuery> atoms = new ReferenceArrayList<AtomQuery>();
	
	public PhraseQuery(Exist exist) {
		super(exist);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "(" + exist + " " + atoms + ")";
	}

	@Override
	public double energy() {
		double ans = 0;
		for (AtomQuery aq : atoms) {
			ans += aq.energy();
		}
		return ans;
	}

	@Override
	public double sumTokenIdf() {
		double ans = 0;
		for (MatcherQuery atom : atoms) {
			ans += atom.sumTokenIdf();
		}
		return ans;
	}

	@Override
	public TokenLiteralQuery findLiteral(MutableString stem) {
		for (AtomQuery atom : atoms) {
			if (atom instanceof TokenLiteralQuery) {
				final TokenLiteralQuery candidate = atom.findLiteral(stem);
				if (candidate != null) {
					return candidate; 
				}
			}
		}
		return null;
	}
}
