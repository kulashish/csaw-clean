package iitb.CSAW.Index;

import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.util.Interval;

public class PhraseWitness extends AWitness {
	/** Ganged with {@link PhraseQuery#atoms} of {@link phrase} */
	public final ReferenceArrayList<AWitness> atomWitnesses = new ReferenceArrayList<AWitness>();
	
	public PhraseWitness(int docId, Interval interval, PhraseQuery phrase) {
		super(docId, interval, phrase);
		this.atomWitnesses.size(phrase.atoms.size());
	}

	@Override
	public String toString() {
		return docId + "_" + interval + "_" + atomWitnesses;
	}

	@Override
	public double energy() {
		final PhraseQuery pq = (PhraseQuery) queryNode;
		if (pq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get energy of NOT match");
		}
		double ans = 0;
		for (AWitness aw : atomWitnesses) {
			ans += aw.energy();
			// TODO FIXME cannot call energy on tbws! this is a bug
		}
		return ans;
	}

	@Override
	public double logEnergy() {
		final PhraseQuery pq = (PhraseQuery) queryNode;
		if (pq.exist == Exist.not) {
			throw new IllegalArgumentException("cannot get logEnergy of NOT match");
		}
		double ans = 0;
		for (AWitness aw : atomWitnesses) {
			ans += aw.logEnergy();
			// TODO FIXME cannot call energy on tbws! this is a bug
		}
		return ans;
	}
}
