package iitb.CSAW.Index;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.ContextQuery.Window;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.util.Interval;

public class ContextWitness extends AWitness {
	public final ReferenceArrayList<AWitness> witnesses = new ReferenceArrayList<AWitness>();
	
	public ContextWitness(int docId, Interval interval, ContextQuery context) {
		super(docId, interval, context);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "_" + docId + interval + "_" + witnesses;
	}

	/**
	 * Separate the witnesses in this query subtree into type bindings and others.
	 * {@link TypeBindingWitness}es should be in the same order as corresponding 
	 * matchers in queryNode, even if queryNode.window = {@link Window#unordered}.
	 */
	public void siftWitnesses(List<TypeBindingWitness> outTbws, List<AWitness> outMws) {
		outTbws.clear();
		outMws.clear();
		for (AWitness aw : witnesses) {
			if (aw instanceof TypeBindingWitness) {
				outTbws.add((TypeBindingWitness) aw);
			}
			else {
				outMws.add(aw);
			}
		}
		final ContextQuery cq = (ContextQuery) queryNode;
		Collections.sort(outTbws, new Comparator<TypeBindingWitness>() {
			@Override
			public int compare(TypeBindingWitness o1, TypeBindingWitness o2) {
				final int i1 = cq.matchers.indexOf(o1.queryNode);
				assert i1 != -1;
				final int i2 = cq.matchers.indexOf(o2.queryNode);
				assert i2 != -1;
				return i1 - i2;
			}
		});
	}

	@Override
	public double energy() {
		throw new UnsupportedOperationException("not supported");
	}

	@Override
	public double logEnergy() {
		throw new UnsupportedOperationException("not supported");
	}
}
