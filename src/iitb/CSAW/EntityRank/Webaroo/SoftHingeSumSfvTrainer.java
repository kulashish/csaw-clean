package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;
import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.EntityRank.SnippetFeatureVector;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.io.InputBitStream;

import java.io.EOFException;
import java.io.File;
import java.io.PrintStream;
import java.util.logging.Level;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.util.MalletLogger;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;

/**
 * Loads {@link SnippetFeatureVector}s to RAM, and applies 
 * {@link LimitedMemoryBFGS} to fit a RankSVM like scoring model with a 
 * soft hinge loss on entity-level feature vectors that are aggregates of all 
 * {@link SnippetFeatureVector}s from snippets supporting each entity.
 * 
 * @author soumen
 */
public class SoftHingeSumSfvTrainer extends AModelGradTrainer {
	/**
	 * @param args [0]=config [1]=log [2]=opcode [3..]=opargs (values of sigma2)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		SoftHingeSumSfvTrainer sfvc = new SoftHingeSumSfvTrainer(conf, 0);
		if (args[2].equals("letor")) {
			PrintStream ps1 = new PrintStream(args[3]);
			sfvc.writeLetorFormatData(ps1);
		}
		else if (args[2].equals("loqcv")) {
			sfvc.leaveOneQueryOut(args, 3);
		}
		// measure pegasos performance  moved to separate class
	}

	/* Should only need summed feature vector per entity, all else should be in base class. */
	DoubleMatrix2D sumSfvs;
	final IntArrayList sumSfvEnts;
	final BooleanArrayList sumSfvLabels;
	final TObjectIntHashMap<String> qidToSumSfvsRowBegin, qidToSumSfvsRowEnd;

	SoftHingeSumSfvTrainer(Config conf, double sigma2) throws Exception {
		super(conf, sigma2);
		MalletLogger.getLogger("edu.umass.cs.mallet.base.ml.maximize.LimitedMemoryBFGS").setLevel(Level.WARNING);
		sumSfvEnts = new IntArrayList();
		sumSfvLabels = new BooleanArrayList();
		qidToSumSfvsRowBegin = new TObjectIntHashMap<String>();
		qidToSumSfvsRowEnd = new TObjectIntHashMap<String>();
		loadLabeledData();
		cleanLabeledData();
		model.size(0);
		model.size(features.size()+1);
		grad.size(0);
		grad.size(features.size()+1);
	}
	
	private SoftHingeSumSfvTrainer(SoftHingeSumSfvTrainer old, double newSigma) {
		super(old, newSigma);
		sumSfvs = old.sumSfvs;
		sumSfvEnts = old.sumSfvEnts;
		sumSfvLabels = old.sumSfvLabels;
		qidToSumSfvsRowBegin = old.qidToSumSfvsRowBegin;
		qidToSumSfvsRowEnd = old.qidToSumSfvsRowEnd;
		model.size(0);
		model.size(features.size()+1);
		grad.size(0);
		grad.size(features.size()+1);
	}

	@Override
	protected AModelGradTrainer flyweightClone(double newSigma2) {
		return new SoftHingeSumSfvTrainer(this, newSigma2);
	}

	/**
	 * Turn into in-memory arrays for quick learning.
	 * @throws Exception
	 */
	void loadLabeledData() throws Exception {
		final File snippetDir = new File(conf.getString(PropertyKeys.snippetDirKey));
		final File sfvFile = new File(snippetDir, PropertyKeys.snippetFeatureVectorPrefix + PropertyKeys.snippetFeatureVectorSuffix);
		final InputBitStream sfvIbs = new InputBitStream(sfvFile, 8192);
		final SnippetFeatureVector sfv = new SnippetFeatureVector();
		String prevQid = null;
		int prevEnt = -1;
		int nSfvs = 0, nQEpairs = 0;
		for (;;) {
			try {
				sfv.load(sfvIbs);
				final String qid = sfv.qid.toString();
				final int qidCmp = prevQid == null? -1 : prevQid.compareTo(qid);
				assert qidCmp <= 0;
				assert prevEnt == -1 || qidCmp < 0 || prevEnt <= sfv.ent;
				if (qidCmp < 0 || prevEnt < sfv.ent) {
					nQEpairs++;
				}
				prevQid = qid;
				prevEnt = sfv.ent;
				++nSfvs;
			}
			catch (EOFException eofx) {
				break;
			}
		}
		logger.info("Scanned " + nSfvs + " sfvs from " + sfvFile + "; " + nQEpairs + " (q,e) pairs");
		sumSfvs = new DenseDoubleMatrix2D(nQEpairs, 1+features.size());
		// second pass to populate
		sfvIbs.position(0);
		int xQEpairs = -1; // will be incr to 0 before first use
		prevQid = null;
		prevEnt = -1;
		for (;;) {
			try {
				sfv.load(sfvIbs);
				final String qid = sfv.qid.toString();
				final int qidCmp = prevQid == null? -1 : prevQid.compareTo(qid);
				assert qidCmp <= 0;
				assert prevEnt == -1 || qidCmp < 0 || prevEnt <= sfv.ent;
				if (qidCmp < 0 || prevEnt < sfv.ent) {
					xQEpairs++;
					sumSfvEnts.add(sfv.ent);
					sumSfvLabels.add(sfv.entLabel);
				}
				if (qidCmp < 0) {
					qidToSumSfvsRowBegin.put(qid, xQEpairs);
					if (prevQid != null) {
						qidToSumSfvsRowEnd.put(prevQid, xQEpairs);
					}
				}
				
				int featurePositionsIndex = 0, currentIndex = 0;
				for (ObjectIterator<Entry> oi = sfv.int2FloatEntrySet().fastIterator(); oi.hasNext(); ) {
					Entry ox = oi.next();
					if(ox.getIntKey() > featurePositions.get(featurePositions.size()-1)){
						break;
					}
					if(currentIndex == featurePositions.get(featurePositionsIndex)){
						sumSfvs.set(xQEpairs, featurePositionsIndex, sumSfvs.get(xQEpairs, featurePositionsIndex) + ox.getFloatValue());
						featurePositionsIndex++;
					}
					currentIndex++;
				}
				sumSfvs.set(xQEpairs, sumSfvs.columns()-1, 1d+sumSfvs.get(xQEpairs, sumSfvs.columns()-1));
				prevQid = qid;
				prevEnt = sfv.ent;
			}
			catch (EOFException eofx) {
				break;
			}
		}
		sfvIbs.close();
		qidToSumSfvsRowEnd.put(prevQid, xQEpairs);
		assert qidToSumSfvsRowBegin.size() == qidToSumSfvsRowEnd.size() : qidToSumSfvsRowBegin.size() + " != " + qidToSumSfvsRowEnd.size();
		assert sumSfvEnts.size() == nQEpairs;
		assert sumSfvLabels.size() == nQEpairs;
		logger.info(qidToSumSfvsRowBegin.size() + " queries, " + sumSfvs.rows() + " (q,e) rows");
	}
	
	void cleanLabeledData() {
		for (int rx = 0; rx < sumSfvs.rows(); ++rx) {
			for (int cx = 0; cx < sumSfvs.columns(); ++cx) {
				assert !Double.isInfinite(sumSfvs.get(rx, cx));
				assert !Double.isNaN(sumSfvs.get(rx, cx));
			}
		}
		logger.info("Checked sumFeatureVector for NaN and inf values");
		// eliminate queries that don't have at least one good and bad ent
		final int preNumQid = qidToSumSfvsRowBegin.size();
		for (TObjectIntIterator<String> qx = qidToSumSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();
			final int rowBegin = qx.value(), rowEnd = qidToSumSfvsRowEnd.get(qid);
			assert rowBegin <= rowEnd;
			double nGood = 0, nBad = 0;
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				if (sumSfvLabels.getBoolean(rx)) {
					nGood++;
				}
				else {
					nBad++;
				}
			}
			if (nGood == 0 || nBad == 0) {
				qidToSumSfvsRowEnd.remove(qid);
				qx.remove();
			}
		}
		assert qidToSumSfvsRowBegin.size() == qidToSumSfvsRowEnd.size();
		logger.info("Thinned down from " + preNumQid + " to " + qidToSumSfvsRowBegin.size() + " queries.");
	}
	
	@Override
	protected void scoreTestEnts(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels) {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		testEntLabels.clear();
		testEntScores.clear();
		final int rowBegin = qidToSumSfvsRowBegin.get(testQid), rowEnd = qidToSumSfvsRowEnd.get(testQid);
		for (int rx = rowBegin; rx < rowEnd; ++rx) {
			final int ent = sumSfvEnts.getInt(rx);
			final boolean entLabel = sumSfvLabels.getBoolean(rx);
			final double entScore = dotprod(forcePositive? expModel : model, sumSfvs, rx);
			assert !testEntScores.containsKey(ent);
			testEntScores.put(ent, entScore);
			assert !testEntLabels.containsKey(ent);
			testEntLabels.put(ent, entLabel? 1 : 0);
		}
	}
	
	/**
	 * {@link LimitedMemoryBFGS} looks for a maximum, not minimum.
	 * Contribution polarities have to be adjusted accordingly. 
	 * @return objective for current {@link #model}.  Also stores
	 * {@link #grad} as a side effect.
	 * If {@link #forcePositive} is true, then
	 * w is constrained to be non negative via transformation
	 * w_j = exp(u_j), where now u is stored in model.
	 * @return objective at w or u
	 */
	@Override
	double objAndGrad() {
		final DoubleArrayList expModel = forcePositive? expModel() : null;
		final int fn = sumSfvs.columns();
		assert fn == grad.size();
		for (int fx = 0; fx < fn; ++fx) {
			grad.set(fx, 0);
		}
		double obj = 0;
		// loss
		for (TObjectIntIterator<String> qx = qidToSumSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();
			if (testQid != null && testQid.equals(qid)) {
				continue;
			}
			final int rowBegin = qx.value(), rowEnd = qidToSumSfvsRowEnd.get(qid);
			assert rowBegin <= rowEnd;
			double nGood = 0, nBad = 0;
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				if (sumSfvLabels.getBoolean(rx)) {
					nGood++;
				}
				else {
					nBad++;
				}
			}
			assert nGood > 0 && nBad > 0;
			double sumGbObj = 0;
			final DoubleArrayList sumGbGrad = new DoubleArrayList();
			sumGbGrad.size(fn);
			final DoubleArrayList fvbMinusfvg = new DoubleArrayList();
			fvbMinusfvg.size(fn);
			for (int gx = rowBegin; gx < rowEnd; ++gx) {
				if (!sumSfvLabels.getBoolean(gx)) continue;
				for (int bx = rowBegin; bx < rowEnd; ++bx) {
					if (sumSfvLabels.getBoolean(bx)) continue;
					fvbMinusfvg.clear();
					fvbMinusfvg.size(fn);
					difference(sumSfvs, bx, gx, fvbMinusfvg);
					final double wdotdiff = dotprod(forcePositive? expModel : model, fvbMinusfvg);
					sumGbObj += softHinge(wdotdiff);
					final double sigmoid = sigmoid(wdotdiff);
					for (int fx = 0; fx < fn; ++fx) {
						final double positiveAdjust = forcePositive? expModel.getDouble(fx) : 1d;
						final double gdelta = positiveAdjust * sigmoid * fvbMinusfvg.getDouble(fx);
						sumGbGrad.set(fx, sumGbGrad.getDouble(fx) + gdelta);
					}
				}
			}
			obj -= sumGbObj / nGood / nBad; // negative polarity
			for (int fx = 0; fx < fn; ++fx) {
				final double delta = sumGbGrad.getDouble(fx) / nGood / nBad;
				grad.set(fx, grad.getDouble(fx) - delta); // negative polarity
			}
		} // for query
		// regularization
		for (int fx = 0; fx < fn; ++fx) {
			obj += -(model.getDouble(fx) * model.getDouble(fx) / 2 / sigma2); // negative polarity
			grad.set(fx, grad.getDouble(fx) - model.getDouble(fx) / sigma2); // negative polarity
		}
		logger.debug("model=" + (forcePositive? expModel : model) + " obj=" + obj + " grad=" + grad);
		return obj;
	}
	
	/**
	 * Create a dump of cleaned dataset, in letor format, at entity (not snippet) level
	 */
	void writeLetorFormatData(PrintStream ps) {
		for (TObjectIntIterator<String> qx = qidToSumSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();			
			final int rowBegin = qx.value(), rowEnd = qidToSumSfvsRowEnd.get(qid);
			assert rowBegin <= rowEnd;	
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				/* pegasos requires 1 and -1 labels */
				int label = sumSfvLabels.getBoolean(rx) == true? 1:-1;
				ps.print(label+" qid:"+rowBegin+" ");					
				for (int c = 0; c < sumSfvs.columns(); c++){
					ps.print((c+1)+":"+sumSfvs.get(rx, c)+" ");
				}			
				ps.println("#"+qid);	
			}
		}
	}
}
