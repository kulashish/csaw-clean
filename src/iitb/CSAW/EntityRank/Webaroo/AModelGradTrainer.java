package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntIterator;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;
import gnu.trove.TObjectLongHashMap;
import gnu.trove.TObjectLongIterator;
import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.EntityRank.SnippetFeatureVector;
import iitb.CSAW.EntityRank.Feature.AFeature;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.AbstractInt2FloatMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.Int2FloatMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.log4j.Logger;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import riso.numerical.LBFGS;
import riso.numerical.LBFGS.ExceptionWithIflag;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.Optimizable;
import cc.mallet.optimize.OptimizationException;
import cc.mallet.optimize.Optimizer;
import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.DenseDoubleMatrix2D;

public abstract class AModelGradTrainer implements Optimizable.ByGradientValue{
	/**
	 * Print stats about the query, ent, snippets workload
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		AModelGradTrainer amgt = new AModelGradTrainer(conf, Double.NaN) {
			@Override
			protected void scoreTestEnts(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels) {
				throw new NotImplementedException();
			}
			@Override
			double objAndGrad() throws IOException {
				throw new NotImplementedException();
			}
			@Override
			protected AModelGradTrainer flyweightClone(double newSigma2) {
				throw new NotImplementedException();
			}
		};
		amgt.cacheSfvsInRam();
		amgt.printStatistics();
	}
	
	private void printStatistics() {
		logger.info("queries " + qidToSfvsRowBegin.size());
		logger.info("total q,e pairs " + ents.size());
		logger.info("ents/query " + 1. * ents.size() / qidToSfvsRowBegin.size());
		logger.info("snippets " + sfvs.rows());
		logger.info("snippets/query " + 1. * sfvs.rows() / qidToSfvsRowBegin.size());
		logger.info("snippets/entity " + 1. * sfvs.rows() / ents.size());
		Monitor snipPerEnt = MonitorFactory.getMonitor("SnipPerEnt", null);
		for (TObjectIntIterator<String> qx = qidToSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();
			final int rowBegin = qx.value(), rowEnd = qidToSfvsRowEnd.get(qid);
			TIntIntHashMap entToNumSnips = new TIntIntHashMap();
			for (int rx = rowBegin; rx < rowEnd; ++rx) {
				final int ent = sfvEnts.getInt(rx);
				entToNumSnips.adjustOrPutValue(ent, 1, 1);
			}
			for (TIntIntIterator e2nsx = entToNumSnips.iterator(); e2nsx.hasNext(); ) {
				e2nsx.advance();
				snipPerEnt.add(e2nsx.value());
			}
		}
		logger.info("snippets/entity " + snipPerEnt);
	}

	
	static final int FILEBUF = 8192;
	static final int clipRankCutOff = 10;
	
	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final File snippetDir, sfvFile;
	final ArrayList<AFeature> features;
	final ArrayList<Integer> featurePositions;
	final double sigma2;

	/* For slow disk scans in case of really big data */
	final TObjectLongHashMap<String> qidToBitBegin;
	final TObjectLongHashMap<String> qidToBitEnd;
	/* When we cache the data in RAM ... */
	DoubleMatrix2D sfvs;
	final IntArrayList sfvEnts;
	final BooleanArrayList sfvEntLabels; 
	final TObjectIntHashMap<String> qidToSfvsRowBegin, qidToSfvsRowEnd;
	
	/*
	 * 2012/02/02
	 * Also create ganged vectors of entity IDs and labels, together
	 * with hash maps from qids to contiguous segments in those vectors.
	 * Note that the qid order here is not necessarily the same as in sfvs!
	 * For each query, an ent will appear exactly once.
	 * This can be used to implement sum-sfv for each ent, or dSM(ent)/dw
	 * in a separate dense array field in subclasses.
	 * Such an array should be small compared to sfvs above.
	 */
	final IntArrayList ents;
	final BooleanArrayList entLabels;
	final TObjectIntHashMap<String> qidToEntsBegin, qidToEntsEnd;
	final IntArrayList sfvRowToEntsRow;

	/** Is the model vector constrained to be nonnegative? */
	static final boolean forcePositive = true;

	/** Note that their sizes can be different from {@link #features} */
	final DoubleArrayList model = new DoubleArrayList(), grad = new DoubleArrayList();
	final Accuracy.All summary;
	int numQueriesDone = 0;
	String testQid = null;
	private double sumPairSwap = 0;

	/* To be implemented by subclasses */
	abstract protected AModelGradTrainer flyweightClone(double newSigma2);
	abstract protected void scoreTestEnts(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels);
	abstract double objAndGrad() throws IOException;
	
	/**
	 * A flyweight clone for members known here.
	 * @param old
	 * @param sigma2
	 */
	protected AModelGradTrainer(AModelGradTrainer old, double sigma2) {
		// following are shallow copied
		conf = old.conf;
		snippetDir = old.snippetDir;
		sfvFile = old.sfvFile;
		features = old.features;
		featurePositions = old.featurePositions;
		this.sigma2 = sigma2;
		qidToBitBegin = old.qidToBitBegin;
		qidToBitEnd = old.qidToBitEnd;
		// sfv part
		sfvs = old.sfvs;
		sfvEnts = old.sfvEnts;
		sfvEntLabels = old.sfvEntLabels;
		qidToSfvsRowBegin = old.qidToSfvsRowBegin;
		qidToSfvsRowEnd = old.qidToSfvsRowEnd;
		// ents part
		ents = old.ents;
		entLabels = old.entLabels;
		qidToEntsBegin = old.qidToEntsBegin;
		qidToEntsEnd = old.qidToEntsEnd;
		sfvRowToEntsRow = old.sfvRowToEntsRow;
		// rest are constructed fresh
		summary = new Accuracy.All(qidToBitBegin.size(), clipRankCutOff);
		model.size(features.size());
		grad.size(features.size());
	}

	@SuppressWarnings("unchecked")
	protected AModelGradTrainer(Config conf, double sigma2) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		this.conf = conf;
		snippetDir = new File(conf.getString(PropertyKeys.snippetDirKey));
		sfvFile = new File(snippetDir, PropertyKeys.snippetFeatureVectorPrefix + PropertyKeys.snippetFeatureVectorSuffix);

		// Read number of partitions
		final int positionSlots = Integer.parseInt(conf.getString(PropertyKeys.proximitySlots)); 
		final int valueSlots = Integer.parseInt(conf.getString(PropertyKeys.idfSlots));
		int numFeaturesCounted = 0;
		
		// Read features which are present in the data, and track their position
		List<Object> highLevelFeatureNames = conf.getList(iitb.CSAW.EntityRank.PropertyKeys.snippetHighLevelFeaturesKey);
		TObjectIntHashMap<String> fnameToIndex = new TObjectIntHashMap<String>();
		for (String featureName : (List<String>)(Object)highLevelFeatureNames) {
			fnameToIndex.put(featureName, numFeaturesCounted);
			if( featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityGridCell.class.getSimpleName()) ||
					featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityRectangleCell.class.getSimpleName()) ){
				numFeaturesCounted += positionSlots * valueSlots;
			}else{
				numFeaturesCounted++;
			}
		}
		
		// Read features which we want for this experiment
		List<Object> featureNames = conf.getList(iitb.CSAW.EntityRank.PropertyKeys.snippetFeaturesKey);
		features = new ArrayList<AFeature>();
		featurePositions = new ArrayList<Integer>();
		for (String featureName : (List<String>)(Object)featureNames) {
			if( featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityGridCell.class.getSimpleName()) || 
					featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityRectangleCell.class.getSimpleName()) ){
				// parameterized features
				String[] parts = featureName.split("\\(");
				assert parts.length == 2;
				String parameterizedfeatureName = parts[0];
				String[] coordinates = parts[1].substring(0,parts[1].indexOf(")")).split(":");
				int row = Integer.parseInt(coordinates[0]); 
				int col = Integer.parseInt(coordinates[1]);
				features.add((AFeature) Class.forName(parameterizedfeatureName).getConstructor(Config.class, int.class, int.class).newInstance(conf,row,col));
				featurePositions.add(fnameToIndex.get(parameterizedfeatureName) + positionSlots * row + col);
			}else{
				// non-parameterized features
				features.add((AFeature) Class.forName(featureName).getConstructor(Config.class).newInstance(conf));
				featurePositions.add(fnameToIndex.get(featureName));
			}
		}
		
		this.sigma2 = sigma2;
		qidToBitBegin = new TObjectLongHashMap<String>();
		qidToBitEnd = new TObjectLongHashMap<String>();
		// sfv part
		sfvEnts = new IntArrayList();
		sfvEntLabels = new BooleanArrayList();
		qidToSfvsRowBegin = new TObjectIntHashMap<String>();
		qidToSfvsRowEnd = new TObjectIntHashMap<String>();
		// ent part
		ents = new IntArrayList();
		entLabels = new BooleanArrayList();
		qidToEntsBegin = new TObjectIntHashMap<String>();
		qidToEntsEnd = new TObjectIntHashMap<String>();
		sfvRowToEntsRow = new IntArrayList();
		
		final InputBitStream sfvIbs = new InputBitStream(sfvFile, FILEBUF);
		final SnippetFeatureVector sfv = new SnippetFeatureVector();
		String prevQid = null;
		int nSfv = 0;
		for (;;) {
			try {
				final long preBit = sfvIbs.readBits();
				sfv.load(sfvIbs);
				++nSfv;
				final String qid = sfv.qid.toString();
				final int qidCmp = prevQid == null? -1 : prevQid.compareTo(qid);
				assert qidCmp <= 0;
				if (qidCmp < 0 && prevQid != null) {
					qidToBitEnd.put(prevQid, preBit);
				}
				if (qidCmp < 0) {
					qidToBitBegin.put(qid, preBit);
				}
				prevQid = qid;
			}
			catch (EOFException eofx) {
				break;
			}
		}
		if (prevQid != null) {
			qidToBitEnd.put(prevQid, sfvIbs.readBits());
		}
		logger.info("rows=" + nSfv + " cols=" + features.size() + " cells=" + nSfv * features.size());
		logger.info("Queries before cleanup " + qidToBitBegin.size() + "=" + qidToBitEnd.size());
		assert qidToBitBegin.size() == qidToBitEnd.size();
		// only queries with both pos and neg ents can be used
		for (TObjectLongIterator<String> qbx = qidToBitBegin.iterator(); qbx.hasNext(); ) {
			qbx.advance();
			final String qid = qbx.key();
			final long bitBegin = qbx.value();
			final long bitEnd = qidToBitEnd.get(qid);
			assert bitEnd > bitBegin;
			sfvIbs.position(bitBegin);
			sfvIbs.readBits(0);
			int numGoodEnt = 0, numBadEnt = 0;
			while (sfvIbs.readBits() < bitEnd - bitBegin) {
				sfv.load(sfvIbs);
				if (sfv.entLabel) {
					++numGoodEnt;
				}
				else {
					++numBadEnt;
				}
			}
			if (numGoodEnt == 0 || numBadEnt == 0) { // cannot use this query
				qbx.remove();
				qidToBitEnd.remove(qid);
			}
		}		
		sfvIbs.close();
		logger.info("Queries after cleanup " + qidToBitBegin.size() + "=" + qidToBitEnd.size());
		assert qidToBitBegin.size() == qidToBitEnd.size();
		summary = new Accuracy.All(qidToBitBegin.size(), clipRankCutOff);
		model.size(features.size());
		grad.size(features.size());
	}
	
	protected void cacheSfvsInRam() throws IOException {
		// count number of rows needed in sfv matrix
		int nrows = 0;
		final InputBitStream sfvIbs = new InputBitStream(sfvFile, FILEBUF);
		final SnippetFeatureVector sfv = new SnippetFeatureVector();
		for (TObjectLongIterator<String> qbx = qidToBitBegin.iterator(); qbx.hasNext(); ) {
			qbx.advance();
			final String qid = qbx.key();
			final long bitBegin = qbx.value();
			final long bitEnd = qidToBitEnd.get(qid);
			assert bitEnd > bitBegin;
			sfvIbs.position(bitBegin);
			sfvIbs.readBits(0);
			while (sfvIbs.readBits() < bitEnd - bitBegin) {
				sfv.load(sfvIbs);
				++nrows;
			}
		}
		// allocate matrix
		logger.info("Allocating " + (nrows * features.size()) + " feature matrix elements");
		sfvs = new DenseDoubleMatrix2D(nrows, features.size());
		// another pass to populate matrix
		for (TObjectLongIterator<String> qbx = qidToBitBegin.iterator(); qbx.hasNext(); ) {
			qbx.advance();
			final String qid = qbx.key();
			final long bitBegin = qbx.value();
			final long bitEnd = qidToBitEnd.get(qid);
			assert bitEnd > bitBegin;
			sfvIbs.position(bitBegin);
			sfvIbs.readBits(0);
			qidToSfvsRowBegin.put(qid, sfvEnts.size());
			while (sfvIbs.readBits() < bitEnd - bitBegin) {
				sfv.load(sfvIbs);
				final int rx = sfvEnts.size();
				int featurePositionsIndex = 0, currentIndex = 0;
				for (ObjectIterator<Entry> oi = sfv.int2FloatEntrySet().fastIterator(); oi.hasNext(); ) {
					if(featurePositionsIndex == featurePositions.size()){
						break;
					}
					final Entry ox = oi.next();
					if(currentIndex == featurePositions.get(featurePositionsIndex)){
						sfvs.set(rx, featurePositionsIndex, ox.getFloatValue());
						featurePositionsIndex++;
					}
					currentIndex++;
				}
				sfvEnts.add(sfv.ent);
				sfvEntLabels.add(sfv.entLabel);
				assert sfvEntLabels.size() == sfvEnts.size();
			}
			qidToSfvsRowEnd.put(qid, sfvEnts.size());
		}
		sfvIbs.close();
		assert sfvEntLabels.size() == sfvEnts.size();
		assert sfvEntLabels.size() == sfvs.rows();
		assert qidToSfvsRowBegin.size() == qidToSfvsRowEnd.size();
		logger.info("Cached SFVs for " + qidToSfvsRowBegin.size() + " queries into RAM, " + sfvEnts.size() + " sfvs.");
		{
			BooleanArrayList present = new BooleanArrayList();
			present.size(sfvEnts.size());
			for (TObjectIntIterator<String> qx = qidToSfvsRowBegin.iterator(); qx.hasNext(); ) {
				qx.advance();
				final String qid = qx.key();
				assert qidToSfvsRowEnd.containsKey(qid);
				final int sfvRowBegin = qx.value(), sfvRowEnd = qidToSfvsRowEnd.get(qid);
				for (int sfvRx = sfvRowBegin; sfvRx < sfvRowEnd; ++sfvRx) {
					present.set(sfvRx, true);
				}
			}
			for (int px = 0, pn = present.size(); px < pn; ++px) {
				assert present.getBoolean(px);
			}
		}
		
		// prepare entity-level vectors and hash maps
		sfvRowToEntsRow.size(sfvEnts.size());
		Arrays.fill(sfvRowToEntsRow.elements(), 0, sfvRowToEntsRow.size(), -1);
		for (TObjectIntIterator<String> qx = qidToSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();
			assert qidToSfvsRowEnd.containsKey(qid);
			final int sfvRowBegin = qx.value(), sfvRowEnd = qidToSfvsRowEnd.get(qid);
			final TIntHashSet queryEnts = new TIntHashSet();
			qidToEntsBegin.put(qid, ents.size());
			for (int sfvRx = sfvRowBegin; sfvRx < sfvRowEnd; ++sfvRx) {
				final int inEnt = sfvEnts.getInt(sfvRx);
				final boolean inEntLabel = sfvEntLabels.getBoolean(sfvRx);
				if (!queryEnts.contains(inEnt)) { // only if ent is new for qid
					ents.add(inEnt);
					entLabels.add(inEntLabel);
					queryEnts.add(inEnt);
				}
				sfvRowToEntsRow.set(sfvRx, ents.size()-1);
			} // sfv rows within query row block
			qidToEntsEnd.put(qid, ents.size());
		} // end query loop
		assert ents.size() == entLabels.size();
		assert qidToEntsBegin.size() == qidToEntsEnd.size();
		assert qidToSfvsRowBegin.size() == qidToEntsBegin.size();
		int numNoInit = 0;
		for (int s2ex = 0, s2en = sfvRowToEntsRow.size(); s2ex < s2en; ++s2ex) {
			if (sfvRowToEntsRow.getInt(s2ex) < 0) {
//				logger.warn(s2ex + " -> " + sfvRowToEntsRow.getInt(s2ex));
				++numNoInit;
			}
		}
		logger.warn(numNoInit + "/" + sfvRowToEntsRow.size() + " not initialized in sfvRowToEntsRow");
		assert numNoInit == 0;
		for (TObjectIntIterator<String> qx = qidToSfvsRowBegin.iterator(); qx.hasNext(); ) {
			qx.advance();
			final String qid = qx.key();
			final int sfvRowBegin = qx.value(), sfvRowEnd = qidToSfvsRowEnd.get(qid);
			for (int sfvRx = sfvRowBegin; sfvRx < sfvRowEnd; ++sfvRx) {
				final int entRx = sfvRowToEntsRow.getInt(sfvRx);
				if (qidToEntsBegin.get(qid) > entRx || entRx >= qidToEntsEnd.get(qid)) {
					logger.warn(qid + " " + qidToEntsBegin.get(qid) + " " + entRx + " " + qidToEntsEnd.get(qid));
				}
			}			
		}
		logger.info("Prepared entity-level vector with " + ents.size() + " q,e rows");
	}
	
	void leaveOneQueryOut(final String[] args, int from) throws Exception {
		WorkerPool wp = new WorkerPool(this, conf.getInt(Config.nThreadsKey));
		for (int ac = from; ac < args.length; ++ac) {
			final int ac_ = ac; // silly
			wp.add(new IWorker() {
				final AModelGradTrainer trainerClone = flyweightClone(Double.parseDouble(args[ac_]));
				
				@Override
				public Exception call() throws Exception {
					logger.info("Worker " + trainerClone.sigma2);
					try {
						trainerClone.leaveOneQueryOut();
//						trainerClone.leaveOneQueryOutRiso();
					}
					catch (Exception anyx) {
						anyx.printStackTrace();
						throw anyx;
					}
					catch (Error anye) {
						anye.printStackTrace();
						throw anye;
					}
					return null;
				}
				
				@Override
				public long numDone() {
					return trainerClone.numQueriesDone;
				}
			});
		}
		wp.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
	}
	
	protected void leaveOneQueryOutRiso() throws IOException, ExceptionWithIflag {
		int qidInt = -1;
		for (TObjectLongIterator<String> tx = qidToBitBegin.iterator(); tx.hasNext(); ) {
			tx.advance();
			testQid = tx.key();
			final LBFGS lbfgs = new LBFGS();
			final int memory = 3, iflag[] = { 0 }, iprint[] = { -1, 0 };
			final double eps = 1e-3, xtol = 1e-9;
			final double[] tempm = new double[this.model.size()];
			final MutableDouble objective = new MutableDouble();
			int nIter = 0;
			for (boolean converged = false; !converged; ++nIter) {
				try {
					objective.setValue(-objAndGrad());
					for (int gx = 0, gn = grad.size(); gx < gn; ++gx) {
						grad.set(gx, -grad.getDouble(gx));
					}
					logger.trace("iter=" + nIter + " obj=" + objective + " model=" + model + " grad=" + grad);
//					logger.trace("iter=" + nIter + " obj=" + objective + " model2=" + l2norm(model) + " grad2=" + l2norm(grad));
					lbfgs.lbfgs(model.size(), memory, model.elements(), objective.doubleValue(), grad.elements(), false, tempm, iprint , eps , xtol , iflag);
				}
				catch (LBFGS.ExceptionWithIflag lbfgsEx) {
					// small crud to deal with bad line-search error message
					String errmsg = lbfgsEx.toString();
					if ( errmsg.indexOf("iflag == -1") != -1 && errmsg.indexOf("info = 6") != -1 ) {
						break;
					}
					else {
						throw lbfgsEx;
					}
				}
				converged = (iflag[0] == 0);
			}
			++qidInt;
			++numQueriesDone;
			collectTestQueryStats(numQueriesDone, qidInt);
		}
		summary.average();
		logger.info("sigma2 = "+sigma2 + " model=" + (forcePositive? expModel() : model) + " grad=" + grad);
		logSummary(summary);
	}
	
	protected DoubleArrayList expModel() {
		DoubleArrayList ans = new DoubleArrayList();
		for (double mj : model) {
			ans.add(Math.exp(mj));
		}
		return ans;
	}
	
	protected void leaveOneQueryOut() throws IOException {
		/* Integer query ID, used to update Accuracy.All summary */ 
		int qidInt = -1;
		for (TObjectLongIterator<String> tx = qidToBitBegin.iterator(); tx.hasNext(); ) {
			tx.advance();
			testQid = tx.key();
			try {
				final Optimizer optimizer = new LimitedMemoryBFGS(this);
				for (int iters = 0;; ++iters) {
					optimizer.optimize(1);
				}
			}
			catch (OptimizationException optx) {
				logger.warn(testQid + " threw " + optx);
			}
	
			++qidInt;
			++numQueriesDone;
			collectTestQueryStats(numQueriesDone, qidInt);
		} // query loop
	
		/* Aggregate per query performance and print */
		summary.average();
		/* Print average performance (per query) */
		logger.info("sigma2 = "+sigma2);
		logSummary(summary);
		/* Print average followed by querywise stats */
		//	summary.printMaxAccuracyText(ps);
	}
	
	void collectTestQueryStats(int numQueriesDone, int qidInt) {
		final TIntDoubleHashMap testEntScores = new TIntDoubleHashMap();
		final TIntIntHashMap testEntLabels = new TIntIntHashMap();
		scoreTestEnts(testEntScores, testEntLabels);
		final double testQueryEntPairSwaps = entPairSwaps(testEntScores, testEntLabels);
		sumPairSwap  += testQueryEntPairSwaps;
		logger.info("sigma2=" + sigma2 + " done=" + qidInt + "/" + qidToBitBegin.size() + " pairSwap=" + sumPairSwap/numQueriesDone);
		calculateMetricsUpdateSummary(qidInt, sigma2, testEntScores, testEntLabels);
	}
	
	double entPairSwaps(TIntDoubleHashMap testEntScores, TIntIntHashMap testEntLabels) {
		double den = 0, num = 0;
		for (TIntDoubleIterator gx = testEntScores.iterator(); gx.hasNext(); ) {
			gx.advance();
			final int goodEnt = gx.key();
			assert testEntLabels.containsKey(goodEnt);
			if (testEntLabels.get(goodEnt) == 0) continue;
			for (TIntDoubleIterator bx = testEntScores.iterator(); bx.hasNext(); ) {
				bx.advance();
				final int badEnt = bx.key();
				assert testEntLabels.containsKey(badEnt);
				if (testEntLabels.get(badEnt) == 1) continue;
				++den;
				if (gx.value() <= bx.value()) {
					num++;
				}
			}
		}
		return num/den;
	}
	
	double dotprod(DoubleList model, DoubleList vec) {
		assert model.size() == vec.size();
		double ans = 0;
		for (int cx = 0, cn = model.size(); cx < cn; ++cx) {
			ans += model.getDouble(cx) * vec.getDouble(cx);
		}
		return ans;
	}
	
	double dotprod(DoubleList model, AbstractInt2FloatMap fv) {
		assert model.size() == fv.size();
		double ans = 0;
		for (Entry fx : fv.int2FloatEntrySet()) {
			ans += model.getDouble(fx.getIntKey()) * fx.getFloatValue();
		}
		return ans;
	}
	
	double dotprod(DoubleList model, DoubleMatrix2D fvm, int rx) {
		assert model.size() == fvm.columns();
		double ans = 0;
		for (int cx = 0, cn = fvm.columns(); cx < cn; ++cx) {
			ans += model.getDouble(cx) * fvm.get(rx, cx);
		}
		return ans;
	}

	void difference(DoubleMatrix2D mat, int bx, int gx, DoubleArrayList diff) {
		final int fn = mat.columns();
		assert fn == diff.size();
		for (int fx = 0; fx < fn; ++fx) {
			diff.set(fx, mat.get(bx, fx) - mat.get(gx, fx));
		}
	}

	void difference(DoubleList va, DoubleList vb, DoubleList vo) {
		assert va.size() == vb.size();
		assert vb.size() == vo.size();
		for (int cx = 0, cn = va.size(); cx < cn; ++cx) {
			vo.set(cx, va.getDouble(cx) - vb.getDouble(cx));
		}
	}
	
	void scale(DoubleList va, double fac) {
		for (int cx = 0, cn = va.size(); cx < cn; ++cx) {
			va.set(cx, fac * va.getDouble(cx));
		}
	}
	
	/**
	 * @param a
	 * @return log(1 + exp(1+a)) = (1+a) + log(1 + exp(-1-a))
	 */
	double softHinge(double a) {
		if (a > 0) {
			return 1d + a + Math.log(1d + Math.exp(-1d-a));
		}
		else {
			return Math.log(1d + Math.exp(1d+a));
		}
	}
	
	/**
	 * @param a
	 * @return exp(1+a)/(1 + exp(1+a)) = 1 / (1 + exp(-1-a))
	 */
	double sigmoid(double a) {
		if (a > 0) {
			return 1d / (1d + Math.exp(-1d-a));
		}
		else {
			final double s = Math.exp(1d+a);
			return s / (1d + s);
		}
	}

	void logSummary(Accuracy.All acc) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		acc.printText(ps);
		logger.info(baos.toString());
	}
	
//	/**
//	 * Ideally we should reverse polarities, however LBFGSTrainer legacy code also reverses polarities
//	 */
//	public double computeFunctionGradient(double[] lambda, double[] currgrad) {
//		// Update model to that provided by lbfgs code
//		for(int lx = 0; lx < lambda.length; lx++){
//			model.set(lx, lambda[lx]);
//		}
//		
//		// using this model cumpute obj and grad values
//		double obj = 0;
//		try {
//			obj = objAndGrad();
//		} catch (IOException iox) {
//			throw new RuntimeException(iox);
//		}
//		
//		// set the gradient value
//		for (int fx = 0; fx < this.grad.size(); ++fx) {
////			grad[fx] = -1.0 * this.grad.get(fx);
//			currgrad[fx] = this.grad.get(fx);
//		}
//		
////		return -1.0 * obj;
//		return obj;
//	}
	
	
	@Override
	public double getValue() {
		try {
			return objAndGrad();
		} catch (IOException iox) {
			throw new RuntimeException(iox);
		}
	}

	@Override
	public void getValueGradient(double[] buffer) {
		try {
			objAndGrad();
		} catch (IOException iox) {
			throw new RuntimeException(iox);
		}
		System.arraycopy(grad.elements(), 0, buffer, 0, grad.size());		
	}

	@Override
	public int getNumParameters() {
		return model.size();
	}

	@Override
	public double getParameter(int index) {
		return model.getDouble(index);
	}

	@Override
	public void getParameters(double[] buffer) {
		assert buffer.length == model.size();
		System.arraycopy(model.elements(), 0, buffer, 0, model.size());
	}

	@Override
	public void setParameter(int index, double value) {
		model.set(index, value);
	}

	@Override
	public void setParameters(double[] params) {
		assert model.size() == params.length;
		model.clear();
		model.addElements(0, params);
	}
	
	/**
	 * Given scores for each entity, find entity order and calculate pairswap, map, mrr, ndcg etc. Update Accuracy.All summary with the values
	 * @param qidInt : Integer query identifier, used to update Accuracy.all summary  
	 * @param sigma2 : sigma2 parameter, used to update Accuracy.all summary
	 * @param finalEntityScores : TIntDoubleHashMap containing mapping of entity identifier to entity score
	 * @param entLabel : TIntIntHashMap containing mapping of entity identifier to entity label (1 or 0)
	 */
	public void calculateMetricsUpdateSummary(int qidInt, double sigma2, TIntDoubleHashMap finalEntityScores, TIntIntHashMap entLabel){

		/* Get sorted pointers based on scores*/
		class Entity implements Comparable<Object>{
			int pointer;
			double score;			

			public Entity(int pointer, double score) {
				this.pointer = pointer;
				this.score = score;				
			}

			/* Decreasing sort*/
			public int compareTo(Object o) {
				Entity tmp = (Entity)o;
				if(this.score > tmp.score){
					return -1;
				}else if(this.score < tmp.score)
					return 1;
				return 0;
			}
		}

		int position = 0;
		Entity[] entityArray = new Entity[finalEntityScores.size()];
		for (TIntDoubleIterator it = finalEntityScores.iterator(); it.hasNext();){
			it.advance();
			int entId = it.key();
			Entity s = new Entity(entId, finalEntityScores.get(entId));
			entityArray[position++] = s;
		}
		Arrays.sort(entityArray);
		boolean[] currLabels = new boolean[entityArray.length];

		/* Create arrays of addresses and labels for current query */
		int numPos = 0;
		for(int sx = 0; sx < entityArray.length; ++sx){
			currLabels[sx] = entLabel.get(entityArray[sx].pointer) > 0;
			if (currLabels[sx]) { ++numPos; }
//			System.out.println("id:"+entityArray[sx].pointer+" label:"+currLabels[sx]+" posScore:"+entityArray[sx].score);
		}

		/* Using sorted array of scores, get evaluation measures */
		RankEvaluator rev = new RankEvaluator(currLabels, numPos);

		for (int testClipRank = 1; testClipRank <= clipRankCutOff; ++testClipRank) {
			summary.update(testClipRank, sigma2 , rev, qidInt);
		}		
	}
	
}
