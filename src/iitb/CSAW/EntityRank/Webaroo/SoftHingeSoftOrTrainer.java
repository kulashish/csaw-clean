package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntIterator;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TObjectIntIterator;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.io.IOException;

public class SoftHingeSoftOrTrainer extends AModelGradTrainer {
	/**
	 * @param args [0]=config [1]=log [2]=opcode [3..]=opargs (values of sigma2)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		SoftHingeSoftOrTrainer shso = new SoftHingeSoftOrTrainer(conf, Double.NaN);
		if (args[2].equals("loqcv")) {
			shso.cacheSfvsInRam();
			shso.leaveOneQueryOut(args, 3);
		}
	}

	SoftHingeSoftOrTrainer(Config conf, double sigma2) throws Exception {
		super(conf, sigma2);
	}
	
	private SoftHingeSoftOrTrainer(SoftHingeSoftOrTrainer old, double newSigma2) {
		super(old, newSigma2);
	}

	@Override
	protected AModelGradTrainer flyweightClone(double newSigma2) {
		return new SoftHingeSoftOrTrainer(this, newSigma2);
	}

	@Override
	double objAndGrad() throws IOException {
		double obj = 0; // to be maximized, note
		for (int cx = 0, cn = sfvs.columns(); cx < cn; ++cx) {
			grad.set(cx, 0);
		}
		int numTrainQuery = 0;
		// reuse these across queries
		final TIntIntHashMap entLabel = new TIntIntHashMap();
		final TIntHashSet goodEnts = new TIntHashSet(), badEnts = new TIntHashSet();
		final TIntDoubleHashMap entToSoftNor = new TIntDoubleHashMap();
		final TIntObjectHashMap<DoubleArrayList> dSOe_dw = new TIntObjectHashMap<DoubleArrayList>();
		for (TObjectIntIterator<String> qbx = qidToSfvsRowBegin.iterator(); qbx.hasNext(); ) {
			qbx.advance();
			final String qid = qbx.key();
			if (testQid != null && testQid.equals(qid)) {
				continue;
			}
			final int rowBegin = qbx.value(), rowEnd = qidToSfvsRowEnd.get(qid);
			assert rowEnd > rowBegin;
			++numTrainQuery;
			// clear all reused collections
			entLabel.clear();
			entToSoftNor.clear();
			goodEnts.clear();
			badEnts.clear();
			// first row loop -- fill entToSoftNor, goodEnts, badEnts, entLabel
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				final int ent = sfvEnts.getInt(rx);
				final double wDotX = dotprod(model, sfvs, rx);
				final double expWDotX = Math.exp(wDotX);
				final double prXnotEvidence = 1. - 1./(1. + 1./expWDotX);
				assert !Double.isNaN(prXnotEvidence);
				assert !Double.isInfinite(prXnotEvidence);
				if (entToSoftNor.containsKey(ent)) {
					final double oldNprob = entToSoftNor.get(ent);
					final double newNprob = oldNprob * prXnotEvidence;
					assert !Double.isNaN(newNprob);
					assert !Double.isInfinite(newNprob);
					entToSoftNor.put(ent, newNprob);
				}
				else {
					entToSoftNor.put(ent, prXnotEvidence);
				}
				entLabel.put(ent, sfvEntLabels.getBoolean(rx)? 1 : 0);
				if (sfvEntLabels.getBoolean(rx)) {
					goodEnts.add(sfvEnts.getInt(rx));
				}
				else {
					badEnts.add(sfvEnts.getInt(rx));
				}
			}
			// second row loop -- fill dSOe_dw (grad of SO(e) wrt w)
			dSOe_dw.clear();
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				final int ent = sfvEnts.getInt(rx);
				final double wDotX = dotprod(model, sfvs, rx);
				final double expWDotX = Math.exp(wDotX);
				assert entToSoftNor.containsKey(ent);
				final double mult = 1./(1. + 1./expWDotX) * entToSoftNor.get(ent);
				final DoubleArrayList dso_dw;
				if (dSOe_dw.containsKey(ent)) {
					dso_dw = dSOe_dw.get(ent);
				}
				else {
					dso_dw = new DoubleArrayList(grad.size());
					dso_dw.size(grad.size());
					dSOe_dw.put(ent, dso_dw);
				}
				for (int cx = 0, cn = dso_dw.size(); cx < cn; ++cx) {
					final double contrib = mult * sfvs.get(rx, cx);
					dso_dw.set(cx, dso_dw.get(cx) + contrib);
				}
			}
			// third ent pair loop -- finish up obj and grad
			// remember to convert from softNor to softOr!
			final double numGoodBadPairs = goodEnts.size() * badEnts.size();
			assert numGoodBadPairs > .9;
			assert entLabel.size() == entToSoftNor.size();
			final DoubleArrayList dso_dw_diff = new DoubleArrayList(); // reused for pairs
			dso_dw_diff.size(grad.size());
			for (TIntIntIterator elxg = entLabel.iterator(); elxg.hasNext(); ) {
				elxg.advance();
				if (elxg.value() != 1) continue;
				final int entg = elxg.key();
				final double gSoftNor = entToSoftNor.get(entg);
				assert dSOe_dw.containsKey(entg);
				final DoubleArrayList dSOg_dw = dSOe_dw.get(entg);
				for (TIntIntIterator elxb = entLabel.iterator(); elxb.hasNext(); ) {
					elxb.advance();
					if (elxb.value() != 0) continue;
					final int entb = elxb.key();
					final double bSoftNor = entToSoftNor.get(entb);
					// obj = softHinge(bSoftOr - gSoftOr) = softHinge(gSoftNor - bSoftNor)
					final double oneTrainingLoss = softHinge(gSoftNor - bSoftNor);
					obj -= oneTrainingLoss / numGoodBadPairs; // note obj sign flipped here
					// grad
					final DoubleArrayList dSOb_dw = dSOe_dw.get(entb);
					final double logit = sigmoid(gSoftNor - bSoftNor);
					difference(dSOb_dw, dSOg_dw, dso_dw_diff);
					scale(dso_dw_diff, logit / numGoodBadPairs);
					for (int cx = 0, cn = grad.size(); cx < cn; ++cx) {
						grad.set(cx, grad.getDouble(cx) - dso_dw_diff.getDouble(cx)); // note sign flipped here
					}
				} // bad
			} // good
		} // query
		// regularization
		for (int fx = 0, fn = model.size(); fx < fn; ++fx) {
			obj -= (model.getDouble(fx) * model.getDouble(fx) / 2d / sigma2);
			grad.set(fx, grad.getDouble(fx) - model.getDouble(fx) / sigma2);
		}
		logger.debug("numTrain=" + numTrainQuery + " model=" + model + " obj=" + obj + " grad=" + grad);
		return obj;
	}

	@Override
	protected void scoreTestEnts(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels) {
		testEntScores.clear();
		testEntLabels.clear();
		final int rowBegin = qidToSfvsRowBegin.get(testQid), rowEnd = qidToSfvsRowEnd.get(testQid);
		assert rowBegin <= rowEnd;
		for (int rx = rowBegin; rx < rowEnd; ++rx) {
			final int ent = sfvEnts.getInt(rx);
			testEntLabels.put(ent, sfvEntLabels.getBoolean(rx)? 1 : 0);
			final double wDotX = dotprod(model, sfvs, rx);
			final double expWDotX = Math.exp(wDotX);
			final double prXnotEvidence = 1. - 1./(1. + 1./expWDotX);
			assert !Double.isNaN(prXnotEvidence);
			assert !Double.isInfinite(prXnotEvidence);
			if (testEntScores.containsKey(ent)) {
				final double oldNprob = testEntScores.get(ent);
				final double newNprob = oldNprob * prXnotEvidence;
				assert !Double.isNaN(newNprob);
				assert !Double.isInfinite(newNprob);
				testEntScores.put(ent, newNprob);
			}
			else {
				testEntScores.put(ent, prXnotEvidence);
			}
		}
		// reverse softNor to softOr
		for (TIntDoubleIterator tesx = testEntScores.iterator(); tesx.hasNext(); ) {
			tesx.advance();
			tesx.setValue(1. - tesx.value());
		}
	}
}
