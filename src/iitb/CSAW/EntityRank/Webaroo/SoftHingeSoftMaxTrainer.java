package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntIterator;
import gnu.trove.TIntIterator;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TObjectIntIterator;
import gnu.trove.TObjectLongIterator;
import iitb.CSAW.EntityRank.SnippetFeatureVector;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry;
import it.unimi.dsi.io.InputBitStream;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import cc.mallet.optimize.Optimizable;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;

public class SoftHingeSoftMaxTrainer extends AModelGradTrainer implements Optimizable.ByGradientValue {
	/**
	 * @param args [0]=config [1]=log [2]=opcode [3..]=optargs
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		if (args[2].equals("letor")) {
			SoftHingeSoftMaxTrainer shsmtr = new SoftHingeSoftMaxTrainer(conf, 0);
			final PrintStream ps = new PrintStream(args[3]);
			shsmtr.writeLetorFormatData(ps);
			ps.close();
		}
		else if (args[2].equals("loqcv")) {
			SoftHingeSoftMaxTrainer shsmtr = new SoftHingeSoftMaxTrainer(conf, 0);
			shsmtr.leaveOneQueryOut(args, 3);
		}
	}
	
	abstract class Softener {
		double objPart, gradPart;
		abstract void run(double raw);
	}
	
	class ExpSoftener extends Softener {
		@Override
		void run(double raw) {
			objPart = gradPart = Math.exp(raw);
		}
	}
	
	class LogSoftener extends Softener {
		@Override
		void run(double raw) {
			if (raw >= 0) {
				objPart = Math.log(1+raw);
				gradPart = 1. / (1. + raw);
			}
			else {
				objPart = - Math.log(1-raw);
				gradPart = 1. / (1. - raw);
			}
		}
	}

	final Softener softener = new LogSoftener();
	DoubleArrayList entSM = null;
	DoubleMatrix2D dSMe_dwj = null;

	SoftHingeSoftMaxTrainer(Config conf, double sigma2) throws Exception {
		super(conf, sigma2);
		cacheSfvsInRam();
	}
	
	private SoftHingeSoftMaxTrainer(SoftHingeSoftMaxTrainer old, double newSigma) {
		super(old, newSigma);
		entSM = new DoubleArrayList();
		entSM.size(ents.size()); // should not shallow copy
		dSMe_dwj = new DenseDoubleMatrix2D(ents.size(), features.size()); // should not shallow copy
		logger.info(softener.getClass().getCanonicalName());
		for (int mx = 0; mx < model.size(); ++mx) {
			model.set(mx, .5);  // poly needs non zero w
		}
	}
	
	@Override
	protected AModelGradTrainer flyweightClone(double newSigma2) {
		return new SoftHingeSoftMaxTrainer(this, newSigma2);
	}
	
	/**
	 * Inference/scoring for test query.
	 */
	@Override
	protected void scoreTestEnts(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels) {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		testEntScores.clear();
		testEntLabels.clear();
		final int rowBegin = qidToSfvsRowBegin.get(testQid), rowEnd = qidToSfvsRowEnd.get(testQid);
		assert rowBegin <= rowEnd;
		for (int rx = rowBegin; rx < rowEnd; ++rx) {
			final int ent = sfvEnts.getInt(rx);
			final double wDotX = dotprod(forcePositive? expModel : model, sfvs, rx);
			softener.run(wDotX);
			final double softWDotX = softener.objPart;
			testEntScores.adjustOrPutValue(ent, softWDotX, softWDotX);
			assert !testEntLabels.containsKey(ent) || (testEntLabels.get(ent) == (sfvEntLabels.getBoolean(rx)? 1 : 0));
			testEntLabels.put(ent, sfvEntLabels.getBoolean(rx)? 1 : 0);
		}
	}

	@Override
	double objAndGrad() throws IOException {
		final double obj = objAndGradRamDense();
		return obj;
	}

	/**
	 * Fast version scanning in-memory feature vectors
	 * Uses a dense ent, j -> dSM(ent)/dw_j matrix {@link #dSMe_dwj}.
	 * @return objective
	 * @throws IOException
	 */
	double objAndGradRamDense() {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		// clear away old values
		double obj = 0;
		for (int cx = 0, cn = sfvs.columns(); cx < cn; ++cx) {
			grad.set(cx, 0);
		}
		Arrays.fill(entSM.elements(), 0, entSM.size(), 0);
		for (int rx = 0, rn = dSMe_dwj.rows(); rx < rn; ++rx) {
			for (int cx = 0, cn = dSMe_dwj.columns(); cx < cn; ++cx) {
				dSMe_dwj.setQuick(rx, cx, 0);
			}
		}
		// reuse for each query
		final TIntHashSet goodEntRows = new TIntHashSet(), badEntRows = new TIntHashSet();
		int numTrainQuery = 0;
		for (TObjectIntIterator<String> qbx = qidToSfvsRowBegin.iterator(); qbx.hasNext(); ) { // query loop
			qbx.advance();
			final String qid = qbx.key();
			if (testQid != null && testQid.equals(qid)) {
				continue;
			}
			final int sfvRowBegin = qbx.value(), sfvRowEnd = qidToSfvsRowEnd.get(qid);
			assert sfvRowBegin < sfvRowEnd;
			++numTrainQuery;
			// clear all reused collections
			goodEntRows.clear();
			badEntRows.clear();
			for (int sfvRx = sfvRowBegin; sfvRx < sfvRowEnd; ++sfvRx) { // sfv row loop
				final double wDotX = dotprod(forcePositive? expModel : model, sfvs, sfvRx);
				softener.run(wDotX);
				final double softWDotXobj = softener.objPart;
				final double softWDotXgrad = softener.gradPart;
				final int entRx = sfvRowToEntsRow.getInt(sfvRx);
				assert qidToEntsBegin.get(qid) <= entRx && entRx < qidToEntsEnd.get(qid);
				entSM.set(entRx, entSM.getDouble(entRx) + softWDotXobj);
				for (int cx = 0, cn = dSMe_dwj.columns(); cx < cn; ++cx) {
					final double oldVal = dSMe_dwj.get(entRx, cx);
					final double contrib = softWDotXgrad * sfvs.get(sfvRx, cx);
					final double forceAdjust = forcePositive? expModel.getDouble(cx) : 1d;
					dSMe_dwj.set(entRx, cx, oldVal + forceAdjust * contrib);
				}
				final boolean entLabel = sfvEntLabels.getBoolean(sfvRx);
				if (entLabel) {
					goodEntRows.add(entRx);
				}
				else {
					badEntRows.add(entRx);
				}
			} // sfv row loop
			final double numGoodBadPairs = goodEntRows.size() * badEntRows.size();
			assert numGoodBadPairs > .9;
			for (TIntIterator gx = goodEntRows.iterator(); gx.hasNext(); ) { // good ent loop
				final int goodEntRow = gx.next();
				final double goodEntScore = entSM.getDouble(goodEntRow);
				for (TIntIterator bx = badEntRows.iterator(); bx.hasNext(); ) { // bad ent loop
					final int badEntRow = bx.next();
					final double badEntScore = entSM.getDouble(badEntRow);
					final double softHingeLoss = softHinge(badEntScore - goodEntScore);
					obj -= softHingeLoss / numGoodBadPairs;
					final double sigmoid = sigmoid(badEntScore - goodEntScore);
					final double coeff = sigmoid / numGoodBadPairs;
					for (int cx = 0, cn = grad.size(); cx < cn; ++cx) {
						final double oldVal = grad.getDouble(cx);
						final double contrib = dSMe_dwj.get(badEntRow, cx) - dSMe_dwj.get(goodEntRow, cx);
						grad.set(cx, oldVal - coeff * contrib);
					}
				} // bad ent loop
			} // good ent loop
		} // end query loop
		
		// regularization
		for (int fx = 0, fn = model.size(); fx < fn; ++fx) {
			obj -= (model.getDouble(fx) * model.getDouble(fx) / 2d / sigma2);
			grad.set(fx, grad.getDouble(fx) - model.getDouble(fx) / sigma2);
		}
		logger.debug("sigma2=" + sigma2 + " numTrain=" + numTrainQuery + " model=" + (forcePositive? expModel : model) + " obj=" + obj + " grad=" + grad);
		return obj;
	}
	
	/**
	 * Fast version scanning in-memory feature vectors.
	 * @return objective
	 * @throws IOException
	 */
	double objAndGradRam() {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		double obj = 0;
		for (int cx = 0, cn = sfvs.columns(); cx < cn; ++cx) {
			grad.set(cx, 0);
		}
		int numTrainQuery = 0;
		// reuse these across queries
		final TIntIntHashMap entLabel = new TIntIntHashMap();
		final TIntHashSet goodEnts = new TIntHashSet(), badEnts = new TIntHashSet();
		final TIntDoubleHashMap entToSoftMax = new TIntDoubleHashMap();
		final TIntObjectHashMap<DoubleArrayList> entToSoftMaxFv = new TIntObjectHashMap<DoubleArrayList>();
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
			entToSoftMax.clear();
			entToSoftMaxFv.clear();
			goodEnts.clear();
			badEnts.clear();
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				final int ent = sfvEnts.getInt(rx);
				final double wDotX = dotprod(forcePositive? expModel : model, sfvs, rx);
				softener.run(wDotX);
				final double softWDotXobj = softener.objPart;
				final double softWDotXgrad = softener.gradPart;
				entToSoftMax.adjustOrPutValue(ent, softWDotXobj, softWDotXobj);
				final DoubleArrayList softMaxFv;
				if (entToSoftMaxFv.containsKey(ent)) {
					softMaxFv = entToSoftMaxFv.get(ent);
				}
				else {
					softMaxFv = new DoubleArrayList(grad.size());
					softMaxFv.size(grad.size());
					entToSoftMaxFv.put(ent, softMaxFv);
				}
				for (int cx = 0, cn = softMaxFv.size(); cx < cn; ++cx) {
					final double contrib = softWDotXgrad * sfvs.get(rx, cx);
					final double forceAdjust = forcePositive? expModel.getDouble(cx) : 1d;
					softMaxFv.set(cx, softMaxFv.getDouble(cx) + forceAdjust * contrib);
				}
				entLabel.put(ent, sfvEntLabels.getBoolean(rx)? 1 : 0);
				if (sfvEntLabels.getBoolean(rx)) {
					goodEnts.add(sfvEnts.getInt(rx));
				}
				else {
					badEnts.add(sfvEnts.getInt(rx));
				}
			}
			final double numGoodBadPairs = goodEnts.size() * badEnts.size();
			assert numGoodBadPairs > .9;
			assert entLabel.size() == entToSoftMax.size();
			for (TIntIntIterator elxg = entLabel.iterator(); elxg.hasNext(); ) {
				elxg.advance();
				if (elxg.value() != 1) continue;
				final int entg = elxg.key();
				final DoubleArrayList softMaxFvG = entToSoftMaxFv.get(entg);
				for (TIntIntIterator elxb = entLabel.iterator(); elxb.hasNext(); ) {
					elxb.advance();
					if (elxb.value() != 0) continue;
					final int entb = elxb.key();
					final DoubleArrayList softMaxFvB = entToSoftMaxFv.get(entb);
					obj -= softHinge(entToSoftMax.get(entb) - entToSoftMax.get(entg)) / numGoodBadPairs;
					final double sigmoid = sigmoid(entToSoftMax.get(entb) - entToSoftMax.get(entg));
					final double coeff = sigmoid / numGoodBadPairs;
					assert softMaxFvG.size() == grad.size();
					assert softMaxFvB.size() == grad.size();
					for (int fx = 0, fn = grad.size(); fx < fn; ++fx) {
						grad.set(fx, grad.getDouble(fx) - (softMaxFvB.getDouble(fx)-softMaxFvG.getDouble(fx))*coeff);
					}
				}
			}
		} // for each query
		// regularization
		for (int fx = 0, fn = model.size(); fx < fn; ++fx) {
			obj -= (model.getDouble(fx) * model.getDouble(fx) / 2d / sigma2);
			grad.set(fx, grad.getDouble(fx) - model.getDouble(fx) / sigma2);
		}
		logger.debug("sigma2=" + sigma2 + " numTrain=" + numTrainQuery + " model=" + (forcePositive? expModel : model) + " obj=" + obj + " grad=" + grad);
		return obj;
	}


	/**
	 * Slow version scanning serialized {@link SnippetFeatureVector}s
	 * @return objective
	 * @throws IOException
	 */
	@Deprecated
	double objAndGradDisk() throws IOException {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		double obj = 0;
		for (TObjectLongIterator<String> qbx = qidToBitBegin.iterator(); qbx.hasNext(); ) {
			qbx.advance();
			final String qid = qbx.key();
			if (testQid != null && testQid.equals(qid)) {
				continue;
			}
			final long bitBegin = qbx.value();
			final long bitEnd = qidToBitEnd.get(qid);
			assert bitEnd > bitBegin;
			final SnippetFeatureVector sfv = new SnippetFeatureVector();
			final InputBitStream sfvIbs = new InputBitStream(sfvFile, FILEBUF);
			sfvIbs.position(bitBegin);
			sfvIbs.readBits(0);
			int numSnips = 0;
			double numGoodEnt = 0, numBadEnt = 0;
			final TIntIntHashMap entLabel = new TIntIntHashMap();
			final TIntDoubleHashMap entToSoftMax = new TIntDoubleHashMap();
			final TIntObjectHashMap<DoubleArrayList> entToSoftMaxFv = new TIntObjectHashMap<DoubleArrayList>();
			while (sfvIbs.readBits() < bitEnd - bitBegin) {
				sfv.load(sfvIbs);
				++numSnips;
				final double wDotX = dotprod(forcePositive? expModel : model, sfv);
				softener.run(wDotX);
				final double softWDotXobj = softener.objPart;
				final double softWDotXgrad = softener.gradPart;
				entToSoftMax.adjustOrPutValue(sfv.ent, softWDotXobj, softWDotXobj);
				entLabel.put(sfv.ent, sfv.entLabel? 1:0);
				final DoubleArrayList softMaxFv;
				if (entToSoftMaxFv.containsKey(sfv.ent)) {
					softMaxFv = entToSoftMaxFv.get(sfv.ent);
				}
				else {
					softMaxFv = new DoubleArrayList(grad.size());
					softMaxFv.size(grad.size());
					entToSoftMaxFv.put(sfv.ent, softMaxFv);
				}
				for (Entry sfvx : sfv.int2FloatEntrySet()) {
					final double contrib = softWDotXgrad * sfvx.getFloatValue();
					final double forceAdjust = forcePositive? expModel.getDouble(sfvx.getIntKey()) : 1d;
					softMaxFv.set(sfvx.getIntKey(), softMaxFv.getDouble(sfvx.getIntKey()) + forceAdjust * contrib);
				}
				if (sfv.entLabel) {
					++numGoodEnt;
				}
				else {
					++numBadEnt;
				}
			}
			assert numGoodEnt > 0 && numBadEnt > 0;
			//			logger.info(qid + " numSnips=" + numSnips + " numEnts=" + entToSoftMax.size() + " numGood=" + numGoodEnt + " numBad=" + numBadEnt);
			assert numSnips >= entToSoftMax.size();
			assert entLabel.size() == entToSoftMax.size();
			for (TIntIntIterator elxg = entLabel.iterator(); elxg.hasNext(); ) {
				elxg.advance();
				if (elxg.value() != 1) continue;
				final int entg = elxg.key();
				final DoubleArrayList softMaxFvG = entToSoftMaxFv.get(entg);
				for (TIntIntIterator elxb = entLabel.iterator(); elxb.hasNext(); ) {
					elxb.advance();
					if (elxb.value() != 0) continue;
					final int entb = elxb.key();
					final DoubleArrayList softMaxFvB = entToSoftMaxFv.get(entb);
					obj -= softHinge(entToSoftMax.get(entb) - entToSoftMax.get(entg)) / numGoodEnt / numBadEnt;
					final double sigmoid = sigmoid(entToSoftMax.get(entb) - entToSoftMax.get(entg));
					final double coeff = sigmoid / numGoodEnt / numBadEnt;
					assert softMaxFvG.size() == grad.size();
					assert softMaxFvB.size() == grad.size();
					for (int fx = 0, fn = grad.size(); fx < fn; ++fx) {
						grad.set(fx, grad.getDouble(fx) - (softMaxFvB.getDouble(fx)-softMaxFvG.getDouble(fx))*coeff);
					}
				}
			}
			sfvIbs.close();
		} // end query loop
		// regularization
		for (int fx = 0, fn = model.size(); fx < fn; ++fx) {
			obj -= (model.getDouble(fx) * model.getDouble(fx) / 2d / sigma2);
			grad.set(fx, grad.getDouble(fx) - model.getDouble(fx) / sigma2);
		}
		return obj;
	}

	void writeLetorFormatData(PrintStream ps) throws IOException {
		int qidInt = -1;
		for (TObjectLongIterator<String> tx = qidToBitBegin.iterator(); tx.hasNext(); ) {
			tx.advance();
			qidInt++;
			testQid = tx.key();
			final long bitBegin = qidToBitBegin.get(testQid);
			final long bitEnd = qidToBitEnd.get(testQid);
			assert bitEnd > bitBegin;
			final SnippetFeatureVector sfv = new SnippetFeatureVector();
			InputBitStream sfvIbs = new InputBitStream(sfvFile, FILEBUF);
			sfvIbs.position(bitBegin);
			sfvIbs.readBits(0);
			while (sfvIbs.readBits() < bitEnd - bitBegin) {
				sfv.load(sfvIbs);
				ps.print(sfv.entLabel==true?1:-1);
				ps.print(" qid:"+qidInt+" ");
				int i = 1;
				for (Entry fx : sfv.int2FloatEntrySet()) {
					ps.print(i+":"+fx.getFloatValue()+" ");
					i++;
				}
				ps.println("#"+testQid+"\t"+sfv.ent);
			}
		} // query loop
	}

}
