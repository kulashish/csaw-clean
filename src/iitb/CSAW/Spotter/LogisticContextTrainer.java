package iitb.CSAW.Spotter;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntDoubleProcedure;
import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.MemoryStatus;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableDouble;

import cc.mallet.optimize.LimitedMemoryBFGS;
import cc.mallet.optimize.Optimizable;
import cc.mallet.optimize.Optimizer;

/**
 * At each leaf of the mention trie, learns a logistic regression classifier
 * among the entities (including NA) attached at that leaf.
 * Will be superceded by {@link SignedHashLogisticContextTrainer}
 * or a Vowpal Wabbit based trainer.
 * 
 * @author soumen
 */
public class LogisticContextTrainer extends LogisticContextBase {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log.
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		TokenCountsReader refTcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		final LogisticContextTrainer ct = new LogisticContextTrainer(conf, refTcr);
//		ct.trainLeavesThreaded();
		ct.trainLeavesWorkPool();
	}

	static final String leafGenName = LogisticContextTrainer.class.getCanonicalName() + ".leaf";
	static final int LEAF_BLOCK_SIZE = 1000, MAX_WEIGHTS = 100000000;
	static final long MAX_LEAF_TIME = ProgressLogger.TEN_MINUTES;
	static final int LEAF_ITERATION_CHUNK = 1000;
	static final double regularizer = 0.1; // quite arbitrary
	final ArrayList<Future<ObjectiveAndGradient>> jobs = new ArrayList<Future<ObjectiveAndGradient>>();

	LogisticContextTrainer(Config conf, TokenCountsReader refTcr) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, InstantiationException, ConfigurationException {
		super(conf, refTcr);
	}
	
	void trainLeavesWorkPool() throws IOException, InterruptedException, ExecutionException {
		MemoryStatus ms = new MemoryStatus();
		final ObjectiveAndGradient oag = new ObjectiveAndGradient();
		int nTryLeaf = 0, nOkLeaf = 0;
		
		LeafTrainTaskPool lttp = new LeafTrainTaskPool(conf);
		final IntArrayList jobs = new IntArrayList();
		ProgressLogger pl = new ProgressLogger();
		pl.expectedUpdates = trie.getNumLeaves();
		pl.start();
		for (;;) {
			lttp.takeLeaves(LEAF_BLOCK_SIZE, jobs, stripeManager.myHostStripe());
			if (jobs.isEmpty()) {
				break;
			}
			for (int leafToDo : jobs) {
				try {
					oag.initialize(leafToDo);
					final long beginTime = System.currentTimeMillis();
					logger.info("Training " + oag + " ok=" + nOkLeaf + "/" + nTryLeaf + " [" + new Date(beginTime) + "] " + ms);
					++nTryLeaf;
					final Optimizer optimizer = new LimitedMemoryBFGS(oag);
					for (;;) {
						optimizer.optimize(LEAF_ITERATION_CHUNK);
						if (optimizer.isConverged()) {
							break;
						}
						if (System.currentTimeMillis() > beginTime + MAX_LEAF_TIME) {
							logger.error("TIMEOUT " + leafToDo);
						}
					}
					oag.trainingAccuracy();
					++nOkLeaf;
					lttp.doneLeaf(leafToDo);
				}
				catch (Exception ex) {
					logger.error("L" + leafToDo + " " + Thread.currentThread() + " " + ex);
					continue;
				}
				catch (AssertionError asse) {
					logger.fatal("L" + leafToDo + " " + Thread.currentThread() + " " + asse.getMessage());
					System.exit(-1);
				}
				catch (OutOfMemoryError oom) {
					logger.fatal("L" + leafToDo + " " + Thread.currentThread() + " OOM " + oom.getMessage());
					System.exit(-1);
				}
				pl.update();
				Thread.yield();
			}
		}
		pl.stop();
		pl.done();
		lttp.close();
	}
	
	class ObjectiveAndGradient implements Optimizable.ByGradientValue, Callable<ObjectiveAndGradient> {
		int leaf, nRec, nEnts, nWeights;
		IntList lents;
		TIntIntHashMap sparseToDense;
		/** Reused from leaf to leaf. */
		final DoubleArrayList model = new DoubleArrayList(MAX_WEIGHTS), grad = new DoubleArrayList(MAX_WEIGHTS);

		@Override
		public String toString() {
			return "L" + leaf + ",nRec=" + nRec + ",nEnts=" + nEnts + ",nWts=" + nWeights;
		}
		
		void initialize(int aleaf) throws IOException {
			leaf = aleaf;
			lents = trie.getSortedEntsNa(aleaf);
			nEnts = lents.size();
			sparseToDense = new TIntIntHashMap();
			final ContextRecordCompact crc = new ContextRecordCompact();
			final ContextFeatureVector cfv = new  ContextFeatureVector();
			final DataInputStream crcDis = getLeafStream(aleaf);
			int _nRec = 0;
			if (crcDis != null) {
				for (;;) {
					try {
						crc.load(crcDis);
						++_nRec;
					}
					catch (EOFException eofx) {
						break;
					}
					assert crc.trieLeaf == aleaf;
					assert lents.contains(crc.entId);
					ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
					for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
						cfvx.advance();
						sparseToDense.put(cfvx.key(), sparseToDense.size());
					}
				}
				crcDis.close();
			}
			nRec = _nRec;
			nWeights = nEnts * sparseToDense.size();
			if (nWeights > MAX_WEIGHTS) {
				throw new RuntimeException("Problem size " + nWeights + " too large");
			}
			model.size(nWeights);
			Arrays.fill(model.elements(), 0, model.size(), 0);
			grad.size(nWeights);
			Arrays.fill(grad.elements(), 0, grad.size(), 0);
		}
		
		int sparseToIndex(int ent, int feat) {
			if (!sparseToDense.containsKey(feat)) {
				throw new IllegalArgumentException("F" + feat + " not mapped");
			}
			final int shent = lents.indexOf(ent);
			if (shent < 0) {
				throw new IllegalArgumentException("E" + ent + " not mapped");
			}
			return shent * sparseToDense.get(feat);
		}
		
		@Override
		public double getValue() {
			final MutableDouble objective = new MutableDouble();
			try {
				computeObjectiveAndGradient(objective, grad);
				// switch from min to max for mallet
				objective.setValue(-objective.doubleValue());
				for (int gx = 0; gx < grad.size(); ++gx) {
					grad.set(gx, -grad.getDouble(gx));
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return objective.doubleValue();
		}

		@Override
		public void getValueGradient(double[] buffer) {
			final MutableDouble objective = new MutableDouble();
			try {
				computeObjectiveAndGradient(objective, grad);
				// switch from min to max for mallet
				objective.setValue(-objective.doubleValue());
				for (int gx = 0; gx < grad.size(); ++gx) {
					grad.set(gx, -grad.getDouble(gx));
				}
			} catch  (IOException e) {
				throw new RuntimeException(e);
			}
			System.arraycopy(grad.elements(), 0, buffer, 0, grad.size());
		}

		@Override
		public int getNumParameters() {
			return nWeights;
		}

		@Override
		public double getParameter(int index) {
			return model.getDouble(index);
		}

		@Override
		public void getParameters(double[] buffer) {
			System.arraycopy(model.elements(), 0, buffer, 0, model.size());
		}

		@Override
		public void setParameter(int index, double value) {
			model.set(index, value);
		}

		@Override
		public void setParameters(double[] params) {
			model.clear();
			model.addElements(0, params);
		}

		@Override
		public ObjectiveAndGradient call() throws Exception {
			return this;
		}
		
		/**
		 * Iterates through all {@link ContextRecordCompact}s for current leaf,
		 * computing objective and gradient. 
		 * @param leaf
		 * @param featureMap
		 * @param nRec 
		 * @param model
		 * @param objective output
		 * @param grad output
		 * @throws IOException 
		 */
		void computeObjectiveAndGradient(MutableDouble objective, final DoubleList grad) throws IOException {
			final IntList entsNa = trie.getSortedEntsNa(leaf);
			final int nFeatures = model.size();
			assert nFeatures == grad.size() : "grad.size=" + grad.size() + " != model.size= " + model.size();
			grad.clear();
			grad.size(nFeatures);
			objective.setValue(0);
			final DataInputStream crcDis = getLeafStream(leaf);
			if (crcDis == null) {
				return;
			}
			final ContextRecordCompact crc = new ContextRecordCompact();
			final ContextFeatureVector cfv = new ContextFeatureVector();
			for (;;) {  // for each instance
				try {
					crc.load(crcDis);
				}
				catch (EOFException eofx) {
					break;
				}
				assert crc.trieLeaf == leaf;
				// set up all class labels including NA
				final TIntDoubleHashMap entToDotProd = new TIntDoubleHashMap();
				for (int ent : entsNa) {
					entToDotProd.put(ent, 0);
				}
				if (!entToDotProd.containsKey(crc.entId)) {
					logger.warn("ent=" + crc.entId + " not in candidates L" + leaf + entsNa);
					continue;
				}
				ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
				cfv.ellOneNormalize();
				double w_dot_phi_xi_yi = Double.NaN, maxDotProd = Double.NEGATIVE_INFINITY;
				for (int ent : entToDotProd.keys()) {
					final double w_dot_phi_xi_ent = getDotProd(ent, cfv);
					entToDotProd.put(ent, w_dot_phi_xi_ent);
					maxDotProd = Math.max(maxDotProd, w_dot_phi_xi_ent);
					if (ent == cfv.ent) {
						w_dot_phi_xi_yi = w_dot_phi_xi_ent;
					}
				}
				assert !Double.isNaN(w_dot_phi_xi_yi);
				assert !Double.isInfinite(maxDotProd);
				final double maxDotProd_ = maxDotProd;
				final MutableDouble sum_ent_exp_dotprod = new MutableDouble();
				entToDotProd.forEachKey(new TIntProcedure() {
					@Override
					public boolean execute(int ent) {
						sum_ent_exp_dotprod.add(Math.exp(entToDotProd.get(ent) - maxDotProd_));
						return true;
					}
				});
				assert !Double.isInfinite(sum_ent_exp_dotprod.doubleValue());
				final double log_zxi = maxDotProd + Math.log(sum_ent_exp_dotprod.doubleValue());
				final double log_prob_yi_given_xi = w_dot_phi_xi_yi - log_zxi;
				// update objective
				objective.add(-log_prob_yi_given_xi);
				// update gradient
				entToDotProd.forEachEntry(new TIntDoubleProcedure() {
					@Override
					public boolean execute(final int ent, double w_dot_phi_xi_ent) {
						final double ent_is_yi = ent == crc.entId? 1 : 0;
						final double prob_ent_given_xi = Math.exp(w_dot_phi_xi_ent - log_zxi);
						for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
							cfvx.advance();
							final int feat = cfvx.key();
							final int windex = sparseToIndex(ent, feat);
							grad.set(windex, grad.get(windex) - cfvx.value() * (ent_is_yi - prob_ent_given_xi));
						}
						return true;
					}
				});
			} // end for each instance
			crcDis.close();
			
			// regularize
			for (int mx = 0; mx < model.size(); ++mx) {
				objective.add(.5d * regularizer * nRec * model.getDouble(mx) * model.getDouble(mx));
				grad.set(mx, grad.getDouble(mx) + regularizer * nRec * model.getDouble(mx));
			}
		}
		
		/**
		 * @param entId
		 * @param cfv
		 * @return $w_e \cdot x$
		 */
		private double getDotProd(final int entId, ContextFeatureVector cfv) {
			double ans = 0;
			for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
				cfvx.advance();
				final int feat = cfvx.key();
				final int windex = sparseToIndex(entId, feat);
				final float val = cfvx.value();
				ans += val * model.getDouble(windex);
			}
			return ans;
		}
		
		double l2norm(double[] vec) {
			double ans = 0;
			for (double val : vec) {
				ans += val * val;
			}
			return Math.sqrt(ans);
		}
		
		double trainingAccuracy() throws IOException {
			final IntList entsNa = trie.getSortedEntsNa(leaf);
			final ContextRecordCompact crc = new ContextRecordCompact();
			final ContextFeatureVector cfv = new ContextFeatureVector();
			final DataInputStream crcDis = getLeafStream(leaf);
			double nSpots = 0, nOkSpots = 0;
			for (;;) {  // for each instance
				try {
					crc.load(crcDis);
				}
				catch (EOFException eofx) {
					break;
				}
				assert crc.trieLeaf == leaf;
				final TIntDoubleHashMap entToDotProd = new TIntDoubleHashMap();
				for (int ent : entsNa) {
					entToDotProd.put(ent, 0);
				}
				ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
				cfv.ellOneNormalize();
				double maxDotProd = Double.NEGATIVE_INFINITY;
				int argMaxDotProd = Integer.MIN_VALUE;
				for (int ent : entToDotProd.keys()) {
					final double w_dot_phi_xi_ent = getDotProd(ent, cfv);
					entToDotProd.put(ent, w_dot_phi_xi_ent);
					if (maxDotProd < w_dot_phi_xi_ent) {
						maxDotProd = w_dot_phi_xi_ent;
						argMaxDotProd = ent;
					}
				}
				if (argMaxDotProd == crc.entId) {
					++nOkSpots;
				}
				++nSpots;
			}
			crcDis.close();
			logger.info("L" + leaf + " ents= " + nEnts + " wts= " + nWeights + " instances= " + nRec + " taccuracy= " + nOkSpots/nSpots + " [" + new Date() + "]");
			return nOkSpots / nSpots;
		}
	} // OAG
}
