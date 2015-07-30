package iitb.CSAW.Spotter;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntHashSet;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.Sort.ExternalMergeSort;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class LfemSkipAllocator extends LeafFeatureEntityMaps {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2?]="write|merge"
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		AStripeManager.construct(conf); // set HostName
		final LfemSkipAllocator skipa = new LfemSkipAllocator(conf);
		if (args.length <= 2) {
			skipa.writeSkipRuns();
			skipa.mergeSkipRuns();
		}
		else if (args[2].equals("write")) {
			skipa.writeSkipRuns();
		}
		else if (args[2].equals("merge")) {
			skipa.mergeSkipRuns();
		}
	}

	final Logger logger = Logger.getLogger(getClass());
	final AStripeManager stripeManager;
	final Random random = new Random(getClass().getCanonicalName().hashCode());
	final static String trainWorkloadName = "TrainWorkload", testWorkloadName = "TestWorkload";

	final File tmpDir, trainWorkloadDir, testWorkloadDir;
	final LongArrayList trainWorkloadLeafToBitEnd = new LongArrayList(trie.getNumLeaves());
	final LongArrayList testWorkloadLeafToBitEnd = new LongArrayList(trie.getNumLeaves());
	
	static final int dynProgMatrixMaxCells = 2 * LfemDynProgOrEqui.MAXDYN;
	
	final static String restrictedLeafKey = ".leaf";
	final TIntHashSet restrictedLeaves;
	final boolean isRestricted;
	final static String propsName = "skip.properties";
	
	final double[] candidateSmoothers = new double[] {
		1e-6, 2e-6, 5e-6,			1e-5, 2e-5, 5e-5,
		1e-4, 2e-4, 5e-4,			1e-3, 2e-3, 5e-3,
		1e-2, 2e-2, 5e-2,			1e-1, 2e-1, 5e-1,
		1, 2, 5,					10, 20, 50,
	};
	
	@SuppressWarnings("unchecked")
	LfemSkipAllocator(Config conf) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, InstantiationException, ConfigurationException, EnvironmentLockedException, DatabaseException {
		super(conf);
		stripeManager = AStripeManager.construct(conf);
		tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		
		final String keyPrefix = LeafFeatureEntityMaps.class.getCanonicalName();
		trainWorkloadDir = new File(sampleDir, trainWorkloadName);
		testWorkloadDir = new File(sampleDir, testWorkloadName);
		trainWorkloadLeafToBitEnd.size(trie.getNumLeaves());
		indexWorkload(trainWorkloadDir, trainWorkloadLeafToBitEnd);
		testWorkloadLeafToBitEnd.size(trie.getNumLeaves());
		indexWorkload(testWorkloadDir, testWorkloadLeafToBitEnd);

		// check for restricted run
		if (conf.containsKey(keyPrefix + restrictedLeafKey)) {
			restrictedLeaves = new TIntHashSet();
			for (String restrictedLeafStr : (List<String>)(Object) conf.getList(keyPrefix + restrictedLeafKey)) {
				restrictedLeaves.add(Integer.parseInt(restrictedLeafStr));
			}
			logger.warn("Restricted leaf set " + conf.getList(keyPrefix + restrictedLeafKey));
			isRestricted = true;
		}
		else {
			restrictedLeaves = null;
			isRestricted = false;
		}
		
		if (!skipDir.isDirectory()) {
			skipDir.mkdir();
		}
	}
	
	/**
	 * Used for skip allocation, so no skips/sents yet.
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	synchronized LfemReader openReadNoSkips() throws IOException, ClassNotFoundException {
		if (featEntFreqBuf == null || leafToBitEnd == null) {
			featEntFreqBuf = BinIO.loadBytes(featEntFreqFile);
			logger.info("Allocating L-F-E-M buffer once, " + featEntFreqBuf.length + " bytes, for skip allocation only.");
			leafToBitEnd = (LongArrayList) BinIO.loadObject(leafToBitEndFile);
			collectLeafBits(null);
		}
		return new LfemReader(new InputBitStream(featEntFreqBuf));
	}
	
	DataOutputStream getBufferedRunDos() throws IOException {
		File lfemLsrDos = File.createTempFile(LfemLeafSkipRecord.class.getCanonicalName(), ".dat", skipDir);
		return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(lfemLsrDos)));
	}
	
	/**
	 * Multithreaded edition.  Each {@link IWorker} writes a run of 
	 * {@link LfemLeafSkipRecord} records sorted by {@link LfemLeafSkipRecord#leaf}.
	 * @throws Exception 
	 */
	void writeSkipRuns() throws Exception {
		// note that we do not construct sentFeat, sentBitPlus or leafToSentEnd
		sentFeat = null;
		sentBitPlus = null;
		leafToSentEnd = null;
		final FloatArrayList leafHitProb = new FloatArrayList(trie.getNumLeaves());
		leafHitProb.size(trie.getNumLeaves());
		collectLeafHitProbs(leafHitProb);
		final LongArrayList leafBits = new LongArrayList(trie.getNumLeaves());
		leafBits.size(trie.getNumLeaves());
		openReadNoSkips().close(); // need this to get lfem ready
		collectLeafBits(leafBits);
		
		final IntArrayList leafNumFeats = new IntArrayList(trie.getNumLeaves());
		{
			leafNumFeats.size(trie.getNumLeaves());
			final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
			final InputBitStream trainIbs = new InputBitStream(new File(trainWorkloadDir, lfcr.getClass().getCanonicalName()+".dat"));
			collectLeafNumFeats(leafNumFeats, trainIbs, trainWorkloadLeafToBitEnd);
			trainIbs.close();
		}
		
		final IntArrayList leafBudget = new IntArrayList(trie.getNumLeaves());
		leafBudget.size(trie.getNumLeaves());
		logger.info("Starting outer allocation");
		final int usedBudget1 = allocateOuter(leafHitProb, leafBits, leafNumFeats, globalBudget, leafBudget);
		logger.info("globalBudget=" + globalBudget + " usedBudget=" + usedBudget1);
		final AtomicInteger leafGen = new AtomicInteger(0);
		final int nThreads = conf.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = trie.getNumLeaves();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.itemsName = "leaf";
		pl.displayFreeMemory = true;
		pl.start("Started allocating inner skips.");
		for (int tx = 0; tx < nThreads; ++tx) {
			workerPool.add(new IWorker() {
				final float[] bestCost_ = innerPolicy == InnerPolicy.DynProgOrEqui? new float[dynProgMatrixMaxCells] : null;
				final int[]	bestEll_ = innerPolicy == InnerPolicy.DynProgOrEqui?  new int[dynProgMatrixMaxCells] : null;

				final int CHUNK = 1000;
				final LfemReader lfemr = openReadNoSkips();
				final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
				final InputBitStream trainIbs = new InputBitStream(new File(trainWorkloadDir, lfcr.getClass().getCanonicalName()+".dat"));
				final InputBitStream tuneIbs = new InputBitStream(new File(testWorkloadDir, lfcr.getClass().getCanonicalName()+".dat"));
				final DataOutputStream lfemLsrDos = getBufferedRunDos();
				long workerNumDone = 0, workerNumSkip = 0;
				@Override
				public Exception call() throws Exception {
					try {
						for (;;) {
							final int fromLeaf = leafGen.getAndAdd(CHUNK);
							for (int aleaf = fromLeaf; aleaf < fromLeaf + CHUNK; ++aleaf) {
								if (aleaf >= trie.getNumLeaves()) {
									return null;
								}
								final IntArrayList refFeats = new IntArrayList(), refBitEnds = new IntArrayList();
								buildFeatToBitEndMap(lfemr, aleaf, refFeats, refBitEnds);
								assert refFeats.size() == refBitEnds.size();
								if (refFeats.isEmpty()) {
									logger.debug("L" + aleaf + " has no features, omitting");
									continue;
								}
								final TIntFloatHashMap trainFeatFreqs = new TIntFloatHashMap(), snappedTrainFeatFreqs = new TIntFloatHashMap();
								collectFeatFreqs(trainIbs, lfcr, aleaf, trainWorkloadLeafToBitEnd, trainFeatFreqs);
								snapFeatFreqs(aleaf, refFeats, trainFeatFreqs, snappedTrainFeatFreqs);
								final TIntFloatHashMap tuneFeatFreqs = new TIntFloatHashMap(), snappedTuneFeatFreqs = new TIntFloatHashMap();
								collectFeatFreqs(tuneIbs, lfcr, aleaf, testWorkloadLeafToBitEnd, tuneFeatFreqs);
								snapFeatFreqs(aleaf, refFeats, tuneFeatFreqs, snappedTuneFeatFreqs);
								final double bestSmoother = findBestSmoother(aleaf, refFeats, snappedTrainFeatFreqs, snappedTuneFeatFreqs);
								final DoubleArrayList trainRefProbs = new DoubleArrayList(refFeats.size());
								trainRefProbs.size(refFeats.size());
								applySmoother(aleaf, bestSmoother, refFeats, snappedTrainFeatFreqs, trainRefProbs);
								
								final ILfemInner innerAllocator;
								switch (innerPolicy) {
								case Freq:
									innerAllocator = new LfemFreq();
									break;
								case Equi:
									innerAllocator = new LfemEqui();
									break;
								case EquiAndFreq:
									innerAllocator = new LfemEquiAndFreq();
									break;
								case DynProgOrEqui:
									final LfemDynProgOrEqui dp = new LfemDynProgOrEqui(conf, bestCost_, bestEll_);
									innerAllocator = dp; 
									break;
								default:
									throw new IllegalArgumentException(innerPolicy.toString());
								}
								final IntArrayList skipIndices = new IntArrayList(leafBudget.getInt(aleaf));
								innerAllocator.allocate(LfemSkipAllocator.this, lfemr, aleaf, leafBudget.getInt(aleaf), refFeats, refBitEnds, trainRefProbs, skipIndices);
								workerNumSkip += skipIndices.size();
								final double roughTrainCost = estimateCost(logger, Level.DEBUG, s0, s1, r0, r1, refFeats, refBitEnds, snappedTrainFeatFreqs, skipIndices);
								final double roughTuneCost = estimateCost(logger, Level.DEBUG, s0, s1, r0, r1, refFeats, refBitEnds, snappedTuneFeatFreqs, skipIndices);
								rewriteSkipIndices(aleaf, refFeats, refBitEnds, skipIndices, roughTrainCost, roughTuneCost, lfemLsrDos);
								++workerNumDone;
								pl.update();
							}
						}
					}
					catch (EOFException eofx) {
					}
					catch (Throwable anyx) {
						anyx.printStackTrace();
					}
					finally {
						lfemr.close();
						trainIbs.close();
						tuneIbs.close();
						lfemLsrDos.close();
						logger.info(this + " ran " + workerNumDone + " leaves, wrote " + workerNumSkip + " skips");
					}
					return null;
				}
				@Override
				public long numDone() {
					return workerNumDone;
				}
			});
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, new Runnable() {
			@Override
			public void run() {
				logger.info(MonitorFactory.getMonitor("UnspentBudget", null));
				logger.info(MonitorFactory.getMonitor("PreThinNumFeat", null));
				logger.info(MonitorFactory.getMonitor("PreThinDpSize", null));
				logger.info(MonitorFactory.getMonitor("NumThinned", null));
			}
		});
		pl.stop("Finished allocating inner skips.");
		pl.done();
		logger.info("FRAC_DYN_PROG " + MonitorFactory.getMonitor(LfemDynProgOrEqui.monKeyUsedDp, null));
	}

	private void collectLeafNumFeats(IntArrayList leafNumFeats, InputBitStream wlIbs, LongArrayList leafToWlBitEnd) throws IOException {
		final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = trie.getNumLeaves();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start("Started collecting number of features at each leaf.");
		for (int aleaf = 0, nleaf = trie.getNumLeaves(); aleaf < nleaf; ++aleaf) {
			final TIntHashSet leafFeatSet = new TIntHashSet();
			final long uptoWlBit = leafToWlBitEnd.getLong(aleaf);
			final long fromWlBit = aleaf == 0? 0 : leafToWlBitEnd.getLong(aleaf - 1);
			final long availWlBit = uptoWlBit - fromWlBit;
			wlIbs.readBits(0);
			wlIbs.position(fromWlBit);
			while (availWlBit > 0 && wlIbs.readBits() < availWlBit) {
				try {
					lfcr.load(wlIbs);
					leafFeatSet.add(lfcr.feat);
				}
				catch (EOFException eofx) {
					break;
				}
			}
			leafNumFeats.set(aleaf, leafFeatSet.size());
			pl.update();
		}
		pl.stop("Finished collecting number of features at each leaf.");
		pl.done();
	}

	void indexWorkload(File workloadDir, LongList leafToWorkloadBitEnd) throws IOException {
		TIntHashSet leafSet = new TIntHashSet();
		logger.info("Started in-memory indexing of workload in " + workloadDir);
		assert leafToWorkloadBitEnd.size() == trie.getNumLeaves();
		for (int aleaf = 0, nleaf = leafToWorkloadBitEnd.size(); aleaf < nleaf; ++aleaf) {
			leafToWorkloadBitEnd.set(aleaf, Long.MIN_VALUE);
		}
		final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
		final InputBitStream workloadIbs = new InputBitStream(new File(workloadDir, lfcr.getClass().getCanonicalName() + ".dat"));
		for (int prevLeaf = Integer.MIN_VALUE;;) {
			try {
				final long preBit = workloadIbs.readBits(); 
				lfcr.load(workloadIbs);
				leafSet.add(lfcr.leaf);
				if (lfcr.leaf != prevLeaf && prevLeaf != Integer.MIN_VALUE) {
					leafToWorkloadBitEnd.set(prevLeaf, preBit);
				}
				prevLeaf = lfcr.leaf;
			}
			catch (EOFException eofx) {
				if (prevLeaf != Integer.MIN_VALUE) {
					leafToWorkloadBitEnd.set(prevLeaf, workloadIbs.readBits());
				}
				break;
			}
		}
		workloadIbs.close();
		int nEmptyLeaf = 0;
		for (int aleaf = 0, nleaf = leafToWorkloadBitEnd.size(); aleaf < nleaf; ++aleaf) {
			if (leafToWorkloadBitEnd.getLong(aleaf) < 0) {
				++nEmptyLeaf;
				leafToWorkloadBitEnd.set(aleaf, aleaf == 0? 0 : leafToWorkloadBitEnd.getLong(aleaf - 1));
			}
		}
		logger.info(leafSet.size() + " + " + nEmptyLeaf + " = " + (leafSet.size() + nEmptyLeaf) + " =? " + leafToWorkloadBitEnd.size());
		logger.info("Finished in-memory indexing of workload in " + workloadDir + " with " + nEmptyLeaf + " of " + leafToWorkloadBitEnd.size() + " empty leaves");
	}

	/**
	 * A leaf is allocated skip/s iff it has a non empty block in LFEM.
	 * <b>Note:</b> There may be leaf blocks that are allocated <em>no</em> skip.
	 * @param leafHits dense smoothed multinomial probability vector from payload
	 * @param leafBits bits in LFEM leaf blocks, can be zero for some leaves
	 * @param globalBudget
	 * @param leafBudget
	 */
	int allocateOuter(FloatList leafHits, LongList leafBits, IntArrayList leafNumFeats, int globalBudget, IntList leafBudget) {
		assert leafHits.size() == leafBits.size();
		assert leafBits.size() == leafBudget.size();
		double sumBits = 0, sumHitBit = 0, sumSqrtHitBit = 0, sumNumFeats = 0;
		for (int aleaf = 0, nl = leafBudget.size(); aleaf < nl; ++aleaf) {
			if (leafBits.getLong(aleaf) == 0) {
				continue;  // allocate skips only to non-empty leaf blocks
			}
			sumBits += leafBits.getLong(aleaf);
			final float hitBit = leafHits.getFloat(aleaf) * leafBits.getLong(aleaf); 
			sumHitBit += hitBit;
			sumSqrtHitBit += Math.sqrt(hitBit);
			sumNumFeats += leafNumFeats.getInt(aleaf);
		}
		int usedBudget = 0;
		double fusedBudget = 0;
		for (int aleaf = 0, nl = leafBudget.size(); aleaf < nl; ++aleaf) {
			if (leafBits.getLong(aleaf) == 0) {
				continue;
			}
			switch (outerPolicy) {
			case Bit:
				final double fBitAdd = 1d * leafBits.getLong(aleaf) * globalBudget / sumBits;
				fusedBudget += fBitAdd;
				final int bitAdd = roundRandom(fBitAdd);
				leafBudget.set(aleaf, bitAdd);
				break;
			case HitBit:
				final double fHitBitAdd = 1d * leafBits.getLong(aleaf) * leafHits.getFloat(aleaf) * globalBudget / sumHitBit;
				fusedBudget += fHitBitAdd;
				final int hitBitAdd = roundRandom(fHitBitAdd);
				leafBudget.set(aleaf, hitBitAdd);
				break;
			case SqrtHitBit:
				final double fSqrtHitBitAdd = 1d * Math.sqrt(leafBits.getLong(aleaf) * leafHits.getFloat(aleaf)) * globalBudget / sumSqrtHitBit;
				fusedBudget += fSqrtHitBitAdd;
				final int sqrtHitBitAdd = roundRandom(fSqrtHitBitAdd);
				leafBudget.set(aleaf, sqrtHitBitAdd);
				break;
			case NumFeat:
				final double fNumFeat = 1d * globalBudget * leafNumFeats.getInt(aleaf) / sumNumFeats;
				fusedBudget += fNumFeat;
				final int numFeatAdd = roundRandom(fNumFeat);
				leafBudget.set(aleaf, numFeatAdd);
				break;
			}
			usedBudget += leafBudget.getInt(aleaf);
		}
		logger.info("fusedBudget=" + (long) fusedBudget);
		return usedBudget;
	}
	
	int roundRandom(double alloc) {
		final int maybe = (int) Math.floor(alloc);
		final double frac = alloc - maybe;
		return random.nextDouble() <= frac? maybe + 1 : maybe;
	}

	/**
	 * @param logger
	 * @param prior logging priority
	 * @param s0
	 * @param s1
	 * @param r0
	 * @param r1
	 * @param refFeats
	 * @param refBitEnds
	 * @param featHits these should be <b>unnormalized counts</b>
	 * @param skipIndices
	 * @return
	 */
	static float estimateCost(Logger logger, Priority prior,
			float s0, float s1, float r0, float r1,
			IntList refFeats, IntArrayList refBitEnds, TIntFloatHashMap featHits,
			IntList skipIndices) {
		float objective = 0;
		int prevSkipIndex = 0, prevSkipBitBegin = 0;
		for (int sx : skipIndices) {
			for (int ux = prevSkipIndex; ux < sx; ++ux) {
				final int uxBitBegin = (ux == 0? 0 : refBitEnds.getInt(ux-1));
				final int uxBitsToSeek = uxBitBegin - prevSkipBitBegin;
				final double uxSeekCost = (uxBitsToSeek > 0? s0 + s1 * uxBitsToSeek : 0);
				final int uxBits = refBitEnds.getInt(ux) - uxBitBegin;
				final double uxReadCost = r0 + r1 * uxBits;
				final double uxHit = featHits.get(refFeats.getInt(ux));
				final double uxCost = uxHit * (uxSeekCost + uxReadCost); 
				objective += uxCost;
//				logger.log(prior, (uxBitsToSeek == 0? "! " : "  ") + ux + " " + refFeats.getInt(ux) + " " + objective + " " + uxCost);
			}
			prevSkipIndex = sx;
			prevSkipBitBegin = (sx == 0? 0 : refBitEnds.getInt(sx-1));
		}
		for (int ux = prevSkipIndex; ux < refFeats.size(); ++ux) {
			final int uxBitBegin = (ux == 0? 0 : refBitEnds.getInt(ux-1));
			final int uxBitsToSeek = uxBitBegin - prevSkipBitBegin;
			final double uxSeekCost = (uxBitsToSeek > 0? s0 + s1 * uxBitsToSeek : 0);
			final int uxBits = refBitEnds.getInt(ux) - uxBitBegin;
			final double uxReadCost = r0 + r1 * uxBits;
			final double uxHit = featHits.get(refFeats.getInt(ux));
			final double uxCost = uxHit * (uxSeekCost + uxReadCost);
			objective += uxCost;
//			logger.log(prior, (uxBitsToSeek == 0? "! " : "  ") + ux + " " + refFeats.getInt(ux) + " " + objective + " " + uxCost);
		}
		return objective;
	}

	/**
	 * <b>Note:</b> In the output, bit counting starts at zero for the given aleaf.
	 * We assume each leaf block is at most {@link Integer#MAX_VALUE} bits long.
	 * Output is sorted in increasing feat order by construction because LFEM blocks are, too.
	 */
	void buildFeatToBitEndMap(LfemReader lfemr, int aleaf, IntList outFeat, IntList outBitEnds) throws IOException {
		outFeat.clear();
		outBitEnds.clear();
		final long leafBitBegin = aleaf == 0? 0 : leafToBitEnd.getLong(aleaf-1);
		final long leafBitCount = leafToBitEnd.getLong(aleaf) - (aleaf == 0? 0 : leafToBitEnd.getLong(aleaf-1));
		lfemr.featEntFreqIbs.position(leafBitBegin);
		lfemr.featEntFreqIbs.readBits(0); // this makes bit offsets relative to beginning of leaf block
		int prevFeat = -1;
		while (lfemr.featEntFreqIbs.readBits() < leafBitCount) {
			final long preBitPos = lfemr.featEntFreqIbs.readBits();
			final int fgap = lfemr.featEntFreqIbs.readGamma();
			/* final int segap = */ lfemr.featEntFreqIbs.readGamma();
			/* final int freq = */ lfemr.featEntFreqIbs.readGamma();
			final int feat = prevFeat + fgap;
			if (feat > prevFeat && prevFeat != -1) {
				outFeat.add(prevFeat);
				if (preBitPos > Integer.MAX_VALUE) {
					throw new RuntimeException("Leaf block for L" + aleaf + " longer than " + preBitPos + " bits");
				}
				outBitEnds.add((int) preBitPos);
			}
			prevFeat = feat;
		}
	}

	/**
	 * @param leafHits completely dense because hit probabilities are smoothed out
	 * @throws IOException
	 */
	void collectLeafHitProbs(FloatList leafHits) throws IOException {
		logger.info("Collecting leaf hit probabilities");
		for (int aleaf = 0, nl = leafHits.size(); aleaf < nl; ++aleaf) {
			leafHits.set(aleaf, leafSmoother);
		}
		LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
		InputBitStream workloadIbs = new InputBitStream(new File(trainWorkloadDir, lfcr.getClass().getCanonicalName()+".dat"));
		for (;;) {
			try {
				lfcr.load(workloadIbs);
				leafHits.set(lfcr.leaf, leafHits.getFloat(lfcr.leaf) + lfcr.count);
			}
			catch (EOFException eofx) {
				break;
			}
		}
		workloadIbs.close();
		float l1sum = 0;
		for (float leafHit : leafHits) {
			l1sum += leafHit;
		}
		assert l1sum > 0;
		Monitor leafHitMon = MonitorFactory.getMonitor("LeafHit", null);
		for (int aleaf = 0, nl = leafHits.size(); aleaf < nl; ++aleaf) {
			leafHits.set(aleaf, leafHits.get(aleaf) / l1sum);
			assert !Float.isNaN(leafHits.getFloat(aleaf));
			leafHitMon.add(leafHits.get(aleaf));
		}
		logger.info(leafHitMon);
	}
	
	/**
	 * @param wlIbs
	 * @param lfcr to avoid repeated allocation
	 * @param aleaf
	 * @param leafToWlBitEnd
	 * @param wlFeatProbs output feature counts, <i>not</i> normalized or smoothed
	 * @throws IOException
	 */
	void collectFeatFreqs(InputBitStream wlIbs, LeafFeatureCountRecord lfcr, int aleaf, LongArrayList leafToWlBitEnd, TIntFloatHashMap wlFeatProbs) throws IOException {
		wlFeatProbs.clear();
		final long uptoWlBit = leafToWlBitEnd.getLong(aleaf);
		final long fromWlBit = aleaf == 0? 0 : leafToWlBitEnd.getLong(aleaf - 1);
		final long availWlBit = uptoWlBit - fromWlBit;
		wlIbs.readBits(0);
		wlIbs.position(fromWlBit);
		while (availWlBit > 0 && wlIbs.readBits() < availWlBit) {
			lfcr.load(wlIbs);
			wlFeatProbs.adjustOrPutValue(lfcr.feat, lfcr.count, lfcr.count);
		}
	}
	
	/**
	 * @param aleaf
	 * @param refFeats assume sorted
	 * @param payload input
	 * @param snappedPayload output
	 */
	void snapFeatFreqs(int aleaf, IntArrayList refFeats, TIntFloatHashMap payload, TIntFloatHashMap snappedPayload) {
		snappedPayload.clear();
		final int[] refFeatsElems = refFeats.elements();
		final int nRefFeats = refFeats.size();
		for (TIntFloatIterator plx = payload.iterator(); plx.hasNext(); ) {
			plx.advance();
			final int ret = Arrays.binarySearch(refFeatsElems, 0, nRefFeats, plx.key());
			if (0 <= ret && ret < nRefFeats) {
				snappedPayload.adjustOrPutValue(refFeatsElems[ret], plx.value(), plx.value());
			}
			else {
				final int ins = Math.min(-ret - 1, nRefFeats-1);
				// ret = -ins - 1 or ins = -ret -1; slight lie at last pos
				snappedPayload.adjustOrPutValue(refFeatsElems[ins], plx.value(), plx.value());
			}
		}
	}
	
	double findBestSmoother(int aleaf, IntArrayList refFeats, TIntFloatHashMap trainFeatFreqs, TIntFloatHashMap tuneFeatFreqs) {
		if (trainFeatFreqs.isEmpty()) {
			return 1; // any positive value would do
		}
		// normalize train feat freqs to get unsmoothed prob
		final float inputL1norm;
		{
			float l1norm = 0;
			for (TIntFloatIterator trFfx = trainFeatFreqs.iterator(); trFfx.hasNext(); ) {
				trFfx.advance();
				l1norm += trFfx.value();
			}
			inputL1norm = l1norm;
			assert inputL1norm > 0;
		}
		final double[] refLogProbs = new double[refFeats.size()];
		double bestLogProb = Float.NEGATIVE_INFINITY, bestSmoother = Float.NaN;
		for (double candidateSmoother : candidateSmoothers) {
			// set up log prob array ganged to refFeats with all positive probs
			{
				double l1norm = 0;
				for (int rpx = 0; rpx < refLogProbs.length; ++rpx) {
					final int refFeat = refFeats.getInt(rpx);
					final float trainProb = trainFeatFreqs.get(refFeat) / inputL1norm;
					refLogProbs[rpx] = Math.log(trainProb + candidateSmoother);
					l1norm += trainProb + candidateSmoother;
				}
				assert l1norm > 0;
				final double logL1Norm = Math.log(l1norm);
				for (int rpx = 0; rpx < refLogProbs.length; ++rpx) {
					refLogProbs[rpx] -= logL1Norm;
				}
			}
			double obj = 0;
			{
				for (int rpx = 0; rpx < refLogProbs.length; ++rpx) {
					obj += refLogProbs[rpx] * tuneFeatFreqs.get(refFeats.getInt(rpx));
				}
			}
			assert !Double.isNaN(obj) && !Double.isInfinite(obj);
			if (obj > bestLogProb) {
				bestLogProb = obj;
				bestSmoother = candidateSmoother;
			}
		}
		return bestSmoother;
	}

	/**
	 * @param aleaf
	 * @param asmoother
	 * @param refFeats
	 * @param snappedFreqs input
	 * @param refProbs output, ganged to refFeats
	 */
	void applySmoother(int aleaf, double asmoother, IntList refFeats, TIntFloatHashMap snappedFreqs, DoubleArrayList refProbs) {
		final boolean isInputEmpty = snappedFreqs.isEmpty();
		float inputl1norm = 0;
		for (TIntFloatIterator sppx = snappedFreqs.iterator(); sppx.hasNext(); ) {
			sppx.advance();
			inputl1norm += sppx.value();
		}
		final int nRefFeats = refFeats.size();
		assert refProbs.size() == nRefFeats;
		double outputl1norm = 0;
		for (int rpx = 0; rpx < nRefFeats; ++rpx) {
			final double val = asmoother + (isInputEmpty? 0 : snappedFreqs.get(refFeats.getInt(rpx)) / inputl1norm);
			refProbs.set(rpx, val);
			outputl1norm += val;
		}
		for (int rpx = 0; rpx < nRefFeats; ++rpx) {
			refProbs.set(rpx, refProbs.getDouble(rpx) / outputl1norm);
		}
	}
	
	void rewriteSkipIndices(int aleaf, IntList refFeats, IntList refBitEnds, IntList skipIndices, double trainCost, double testCost, DataOutputStream lfemsrDos) throws IOException {
		assert 0 <= trainCost && !Double.isInfinite(trainCost) && !Double.isNaN(trainCost) : "trainCost=" + trainCost;
		assert 0 <= testCost && !Double.isInfinite(testCost) && !Double.isNaN(testCost) : "testCost=" + testCost;
		final IntArrayList localSkipFeat = new IntArrayList(), localSkipBitPlus = new IntArrayList();
		int prevSkipIndex = Integer.MIN_VALUE;
		for (int skipIndex : skipIndices) {
			assert prevSkipIndex < skipIndex;
			assert localSkipFeat.size() == localSkipBitPlus.size();
			final int chosenSkipFeature = refFeats.getInt(skipIndex);
			final int chosenBitPlus = skipIndex == 0? 0 : refBitEnds.getInt(skipIndex - 1);
			localSkipFeat.add(chosenSkipFeature);
			localSkipBitPlus.add(chosenBitPlus);
			prevSkipIndex = skipIndex;
		}
		new LfemLeafSkipRecord(aleaf, skipIndices.size(), trainCost, testCost, localSkipFeat, localSkipBitPlus).store(lfemsrDos);
	}
	
	void mergeSkipRuns() throws InstantiationException, IllegalAccessException, IOException, ConfigurationException {
		final File mergedOutFile = new File(skipDir, LfemLeafSkipRecord.class.getCanonicalName() + ".dat");
		// merge lfem leaf skip records
		File tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		Comparator<LfemLeafSkipRecord> comparator = new Comparator<LfemLeafSkipRecord>() {
			@Override
			public int compare(LfemLeafSkipRecord o1, LfemLeafSkipRecord o2) {
				return o1.leaf - o2.leaf;
			}
		};
		ExternalMergeSort<LfemLeafSkipRecord> ems = new ExternalMergeSort<LfemLeafSkipRecord>(LfemLeafSkipRecord.class, comparator, false, tmpDir);
		ArrayList<File> runFiles = new ArrayList<File>();
		for (File runFile : skipDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.matches(LfemLeafSkipRecord.class.getCanonicalName() + "\\d+\\.dat");
			}
		})) {
			runFiles.add(runFile);
		}
		if (!runFiles.isEmpty()) {  // because we delete runs after merge
			ems.mergeFanIn(runFiles, mergedOutFile);
		}
		
		// leaf probs will NOT be used to estimate cost
		double estTrainCost = 0, estTestCost = 0;

		// turn merged file of records into skip files LFEM can use
		leafToSentEnd = new IntArrayList();
		leafToSentEnd.size(trie.getNumLeaves());
		for (int lsex = 0, lsen = leafToSentEnd.size(); lsex < lsen; ++lsex) {
			leafToSentEnd.set(lsex, Integer.MIN_VALUE);
		}
		sentFeat = new IntArrayList();
		sentBitPlus = new IntArrayList();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = trie.getNumLeaves();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.itemsName = "leaf";
		pl.displayFreeMemory = true;
		pl.start("Started converting skips to LFEM format.");
		final LfemLeafSkipRecord lfemLsr = new LfemLeafSkipRecord();
		DataInputStream lfemLsrDis = new DataInputStream(new BufferedInputStream(new FileInputStream(mergedOutFile)));
		long nSkips = 0, nRecs = 0;
		try {
			for (int prevLeaf = Integer.MIN_VALUE;;) {
				lfemLsr.load(lfemLsrDis);
				++nRecs;
				assert prevLeaf < lfemLsr.leaf : "L" + prevLeaf + " :: L" + lfemLsr.leaf;
				assert lfemLsr.sentFeat.size() == lfemLsr.sentBitPlus.size();
				assert lfemLsr.sentBitPlus.size() == lfemLsr.usedBudget;
				nSkips += lfemLsr.usedBudget;
				assert sentFeat.size() == sentBitPlus.size();
				sentFeat.addAll(lfemLsr.sentFeat);
				sentBitPlus.addAll(lfemLsr.sentBitPlus);
				leafToSentEnd.set(lfemLsr.leaf, sentFeat.size());
				estTrainCost += lfemLsr.estimatedTrainCost;
				assert estTrainCost >= 0 && !Double.isNaN(estTrainCost) : "estTrainCost=" + estTrainCost;
				estTestCost += lfemLsr.estimatedTestCost;
				assert estTestCost >= 0 && !Double.isNaN(estTestCost) : "estTestCost=" + estTestCost;
				pl.update();
				prevLeaf = lfemLsr.leaf;
			}
		}
		catch (EOFException eofx) { }
		lfemLsrDis.close();
		pl.stop();
		pl.done();
		logger.info("counted skips " + nSkips + " recs " + nRecs);
		logger.info("Skip stats " + leafToSentEnd.size() + " " + sentFeat.size() + " " + sentBitPlus.size());
		// patch up missing items in leafToSentEnd
		int numPatched = 0;
		for (int lsex = 0, lsen = leafToSentEnd.size(); lsex < lsen; ++lsex) {
			if (leafToSentEnd.getInt(lsex) < 0) {
				leafToSentEnd.set(lsex, lsex == 0? 0 : leafToSentEnd.getInt(lsex-1));
				++numPatched;
			}
		}
		logger.info("Patched leafToSentEnd at " + numPatched + " leaves");
		if (isRestricted) {
			logger.warn("Restricted leaf run, not saving to " + skipDir);
			return;
		}
		logger.info("Writing skips to " + skipDir);
		if (!skipDir.isDirectory()) {
			skipDir.mkdir();
		}
		final File leafToSentEndFile = new File(skipDir, leafToSentEndName);
		BinIO.storeObject(leafToSentEnd, leafToSentEndFile);
		final File sentFeatFile = new File(skipDir, sentFeatName);
		BinIO.storeObject(sentFeat, sentFeatFile);
		final File sentBitPlusFile = new File(skipDir, sentBitPlusName);
		BinIO.storeObject(sentBitPlus, sentBitPlusFile);
		assert sentFeat.size() == sentBitPlus.size();
		writePropertiesAudit(sentFeat.size(), estTrainCost, estTestCost);
	}

	private void writePropertiesAudit(int usedBudget, double estTrainCost, double estTestCost) throws ConfigurationException {
		final String keyPrefix = LeafFeatureEntityMaps.class.getCanonicalName();
		PropertiesConfiguration skipProps = new PropertiesConfiguration();
		skipProps.addProperty(PropertyKeys.r0Key, r0);
		skipProps.addProperty(PropertyKeys.r1Key, r1);
		skipProps.addProperty(PropertyKeys.s0Key, s0);
		skipProps.addProperty(PropertyKeys.s1Key, s1);
		skipProps.addProperty(keyPrefix + "." + OuterPolicy.class.getSimpleName(), outerPolicy.toString());
		skipProps.addProperty(keyPrefix + "." + InnerPolicy.class.getSimpleName(), innerPolicy.toString());
		skipProps.addProperty(keyPrefix + "." + ThinPolicy.class.getSimpleName(), thinPolicy.toString());
		skipProps.addProperty(keyPrefix + "." + PermutePolicy.class.getSimpleName(), permutePolicy.toString());
		skipProps.addProperty(keyPrefix + globalBudgetKey, globalBudget);
		skipProps.addProperty(keyPrefix + usedBudgetKey, usedBudget);
		skipProps.addProperty(keyPrefix + estTrainCostKey, estTrainCost);
		skipProps.addProperty(keyPrefix + estTestCostKey, estTestCost);
		skipProps.addProperty(keyPrefix + leafSmootherKey, leafSmoother);
		skipProps.addProperty(keyPrefix + thinBudgetMultipleKey, thinBudgetMultiple);
		skipProps.addProperty(keyPrefix + thinMaxFeatureKey, thinMaxFeature);
		skipProps.addProperty(keyPrefix + thinFreqFractionKey, thinFreqFraction);
		skipProps.save(new File(skipDir, propsName));
	}
}
