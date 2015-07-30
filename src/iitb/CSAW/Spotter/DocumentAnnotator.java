package iitb.CSAW.Spotter;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntFloatIterator;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

/**
 * Must construct one for each thread --- <b>not</b> thread-safe.
 * Not sure this code deserves a separate class.
 * @author soumen
 */
public class DocumentAnnotator extends DocumentSpotter {
	final BayesContextClassifier classifier;
	
	public DocumentAnnotator(Config conf, TokenCountsReader refTcr) throws Exception {
		super(conf, refTcr);
		classifier = new BayesContextClassifier(conf, refTcr);
	}
	
	public void classifyContext(ContextRecordCompact crc, IntList entIds, boolean loo, TIntFloatHashMap scores) throws IOException {
		final BayesContextClassifier.Auto auto = new BayesContextClassifier.Auto();
		classifier.classifyContext(auto, crc, entIds, loo, scores);
	}
	
	/**
	 * Normalizes log probabilities a1 ... an with Z = exp(a1) + ... + exp(an)
	 *   = exp(amax) ( sum_i exp(ai - amax) ).
	 * This isn't coded quite right, should use a heap. <br/>
	 * log Z = amax + log(sum_i exp(ai - amax)).
	 */
	public void normalizeAndSortScores(final TIntFloatHashMap jumbled, IntArrayList sorted) {
		sorted.clear();
		if (jumbled.isEmpty()) { return; }
		double maxLogProb = Double.NEGATIVE_INFINITY;
		for (TIntFloatIterator jx = jumbled.iterator(); jx.hasNext(); ) {
			jx.advance();
			sorted.add(jx.key());
			maxLogProb = (maxLogProb < jx.value())? jx.value() : maxLogProb;
		}
		double sumExpAiMinusAmax = 0;
		for (TIntFloatIterator jx = jumbled.iterator(); jx.hasNext(); ) {
			jx.advance();
			sumExpAiMinusAmax += Math.exp(jx.value() - maxLogProb);
		}
		final double logZee = maxLogProb + Math.log(sumExpAiMinusAmax);
		if (logZee <= -Float.MAX_VALUE || logZee >= Float.MAX_VALUE) {
			throw new ArithmeticException("Magnitude of logZ=" + logZee + " is too large: " + new FloatArrayList(jumbled.getValues()));
		}
		for (TIntFloatIterator jx = jumbled.iterator(); jx.hasNext(); ) {
			jx.advance();
			jx.setValue(jx.value() - (float) logZee);
			assert !Float.isNaN(jx.value());
		}
		Sorting.quickSort(sorted.elements(), 0, sorted.size(), new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final float diff = jumbled.get(o2) - jumbled.get(o1);
				if (diff > 0) return 1;
				else if (diff < 0) return -1;
				else return o1 - o2; // deterministic
			}
		});
	}

	public int numTrainingContexts(int trieLeafNodeId) {
		return classifier.numTrainingContexts(trieLeafNodeId);
	}
}
