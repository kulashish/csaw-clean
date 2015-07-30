package iitb.CSAW.Spotter;

import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntHashSet;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.LongIntInt;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.filefilter.RegexFileFilter;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Run this class with one different candidate Lidstone value on each host.
 * Each host will dump a file.  Check manually to see that all hosts have completed.
 * Then run this class again on any one host without specifying a Lidstone value.
 * Note that the final Lidstone file will be saved in {@link ContextBase#ctxDir2},
 * not in a L-F-E-M specific directory. 
 * @author soumen
 */
public class BayesLidstoneTuner extends ContextBase {
	/**
	 * @param args [0]=/path/to/conf [1]=/path/to/log [2]=lidstone (optional)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final TokenCountsReader tcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		final BayesLidstoneTuner blt = new BayesLidstoneTuner(conf, tcr);
		if (args.length > 2) {
			final float alidstone = Float.parseFloat(args[2]);
			blt.evaluateLidstoneValue(alidstone);
		}
		else {
			blt.reportAggregateAccuracy();
		}
		blt.close();
	}

	final static boolean doLoo = true;
	final static String accuracyNamePrefix = "Accuracy", accuracyNameSuffix = ".dat";
	final Monitor macroAvgAccuracy = MonitorFactory.getMonitor("MacroAvgAccuracy", null);
	final float lidstoneForSmallLeaves = 0.1f;
	
	BayesLidstoneTuner(Config conf, TokenCountsReader tcr) throws Exception {
		super(conf, tcr);
		if (!doLoo) {
			logger.warn("Note: evaluating on training data without L-O-O.");
		}
	}

	/**
	 * Also saves {@link BayesContextBase#leafLidstoneParameterName}.
	 * @throws Exception
	 */
	void reportAggregateAccuracy() throws Exception {
		int leafToNumLidstones[] = new int[trie.getNumLeaves()];
		final float leafToBestLidstone[] = new float[trie.getNumLeaves()];
		Arrays.fill(leafToBestLidstone, Float.NaN);
		final float leafToBestAccuracy[] = new float[trie.getNumLeaves()];
		final float leafToLaplacianAccuracy[] = new float[trie.getNumLeaves()];
		final float[] leafToInstances = new float[trie.getNumLeaves()];
		final float[] leafToCorrectInstances = new float[trie.getNumLeaves()];
		for (File accuracyFile : ctxDir2.listFiles((FileFilter) new RegexFileFilter(accuracyNamePrefix + "_.*\\" + accuracyNameSuffix))) {
			logger.info("Reading " + accuracyFile);
			final DataInputStream accuracyDis = new DataInputStream(new FileInputStream(accuracyFile));
			final BayesLidstoneResult blr = new BayesLidstoneResult();
			for (;;) {
				try {
					blr.load(accuracyDis);
					// we assume each leaf encountered has a different lidstone
					// ie different hosts used different lidstones
					++leafToNumLidstones[blr.aleaf];
					if (leafToBestAccuracy[blr.aleaf] < blr.accuracy) {
						leafToBestAccuracy[blr.aleaf] = blr.accuracy;
						leafToBestLidstone[blr.aleaf] = blr.alidstone;
						leafToInstances[blr.aleaf] = blr.instances;
						leafToCorrectInstances[blr.aleaf] = blr.instances * blr.accuracy;
					}
					if (0.999 < blr.alidstone && blr.alidstone < 1.001) {
						leafToLaplacianAccuracy[blr.aleaf] = blr.accuracy;
					}
				}
				catch (EOFException eofx) {
					break;
				}
			}
			accuracyDis.close();
		}
		// how many leaves encountered? also fill default lidstones for small leaves
		int nPresentLeaves = 0, nLnan = 0;
		for (int lx = 0; lx < leafToNumLidstones.length; ++lx) {
			if (leafToNumLidstones[lx] > 0) {
				++nPresentLeaves;
			}
			if (Float.isNaN(leafToBestLidstone[lx])) {
				++nLnan;
				leafToBestLidstone[lx] = lidstoneForSmallLeaves;
			}
		}
		// for how many leaves did lidstone improve accuracy?
		int bestBetterThanLap = 0;
		for (int lx = 0; lx < leafToBestAccuracy.length; ++lx) {
			if (leafToBestAccuracy[lx] > leafToLaplacianAccuracy[lx]) {
				++bestBetterThanLap;
			}
		}
		// print report
		logger.info("nPresent=" + nPresentLeaves + " nNan=" + nLnan + " nWon=" + bestBetterThanLap + " of " + leafToBestAccuracy.length + " leaves");
		float bestSum = 0, lapSum = 0;
		for (int lx = 0; lx < leafToBestAccuracy.length; ++lx) {
			bestSum += leafToBestAccuracy[lx];
			lapSum += leafToLaplacianAccuracy[lx];
		}
		logger.info("Macroaverage best=" + bestSum/nPresentLeaves + " laplace=" + lapSum/nPresentLeaves);
		// recompute macroaverage to check
		
		// microaverage
		{
			float sumInstances = 0, sumCorrectInstances = 0;
			for (int aleaf = 0; aleaf < leafToInstances.length; ++aleaf) {
				assert leafToInstances[aleaf] >= leafToCorrectInstances[aleaf];
				sumInstances += leafToInstances[aleaf];
				sumCorrectInstances += leafToCorrectInstances[aleaf];
			}
			logger.info("Microaverage " + (sumCorrectInstances / sumInstances));
		}
		
		// save best lidstone params to file 
		final File lidstoneFile = new File(ctxDir2, ALfeMap.leafLidstoneParameterName);
		BinIO.storeFloats(leafToBestLidstone, lidstoneFile);
		logger.info("Wrote " + lidstoneFile);
	}
	
	void evaluateLidstoneValue(float alidstone) throws IOException {
		final File accuracyFile = new File(ctxDir2, accuracyNamePrefix + "_" + stripeManager.myHostName() + accuracyNameSuffix);
		final DataOutputStream accuracyDos;
		accuracyDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(accuracyFile), 32768));
		ProgressLogger pl = new ProgressLogger(logger);
//		pl.expectedUpdates = trie.getNumLeaves();
		pl.displayFreeMemory = true;
		pl.start("Starting evaluation for Lidstone " + alidstone);
		for (int aleaf = 0; aleaf < trie.getNumLeaves(); ++aleaf) {
			evaluateLidstoneValueOneLeaf(pl, aleaf, alidstone, accuracyDos);
//			pl.update();
			if (aleaf % 1000 == 0) {
				logger.info(macroAvgAccuracy);
			}
		}
		pl.stop("Finished evaluation for Lidstone " + alidstone);
		pl.done();
		logger.info(macroAvgAccuracy);
		accuracyDos.close();
	}
	
	void evaluateLidstoneValueOneLeaf(ProgressLogger ppl, int aleaf, float alidstone, DataOutputStream accuracyDos) throws IOException {
		float nCorrectInst = 0;
		
		// first pass to collect counts
		DataInputStream leafDis = getLeafStream(aleaf);
		if (leafDis == null) {
			return;
		}
		final TIntHashSet featSet = new TIntHashSet();
		final Long2FloatOpenHashMap featEntToFreqMap = new Long2FloatOpenHashMap();
		final ContextRecordCompact crc = new ContextRecordCompact();
		final ContextFeatureVector cfv = new ContextFeatureVector();
		final LongIntInt featEnt = new LongIntInt();
		final IntList ents = trie.getSortedEntsNa(aleaf);
		final float[] exToNumInst = new float[ents.size()]; // for priors
		final float[] exToSumFreq = new float[ents.size()];
		float nInst = 0;
		for (;;) {
			try {
				crc.load(leafDis);
				assert crc.trieLeaf == aleaf;
				final int ex = ents.indexOf(crc.entId);
				assert 0 <= ex && ex < ents.size();
				++exToNumInst[ex];
				++nInst;
				ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
				for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
					cfvx.advance();
					final int feat = cfvx.key();
					featEnt.write(feat, crc.entId);
					final int inc = (int) cfvx.value();
					if (featEntToFreqMap.containsKey(featEnt.lv)) {
						featEntToFreqMap.put(featEnt.lv, featEntToFreqMap.get(featEnt.lv) + inc);
					}
					else {
						featEntToFreqMap.put(featEnt.lv, inc);
					}
					featSet.add(feat);
					exToSumFreq[ex] += inc;
				}
			}
			catch (EOFException eofx) {
				break;
			}
		}
		leafDis.close();
		if (nInst <= 1) {
			logger.debug("L" + aleaf + " has only one instance");
			return;
		}
		final double leafVocabSize = featSet.size();

		// finish up log prior
		final float logPrior[] = new float[ents.size()]; // ganged to ents
		{
			final double logDenom = Math.log(nInst + ALfeMap.entPriorLidstone * ents.size());
			for (int ex = 0; ex < ents.size(); ++ex) {
				final double logNumer = Math.log(exToNumInst[ex] + ALfeMap.entPriorLidstone);
				logPrior[ex] = (float) (logNumer - logDenom);
			}
		}
		
		// second pass for lidstone selection
		leafDis = getLeafStream(aleaf);
		final float[] scores = new float[ents.size()]; // ganged to ents reused for each crc
		for (;;) {
			try {
				crc.load(leafDis);
				assert crc.trieLeaf == aleaf;
				final int gtex = ents.indexOf(crc.entId); // ground truth
				assert 0 <= gtex && gtex < ents.size();
				for (int ex = 0; ex < ents.size(); ++ex) {
					scores[ex] = logPrior[ex];
				}
				ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
				for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
					cfvx.advance();
					final int feat = cfvx.key();
					final float testFreq = cfvx.value();
					for (int ex = 0; ex < ents.size(); ++ex) {
						final int ent = ents.getInt(ex);
						featEnt.write(feat, ent);
						final float trainFreq = featEntToFreqMap.containsKey(featEnt.lv)? featEntToFreqMap.get(featEnt.lv) : 0;
						ppl.update();
						if (doLoo && ent == crc.entId) {
							final double logNumer = Math.log(trainFreq - testFreq + alidstone);
							final double logDenom = Math.log(exToSumFreq[ex] - testFreq + alidstone * leafVocabSize);
							assert logNumer <= logDenom : "L" + aleaf + " E" + ent + " train=" + trainFreq + " test=" + testFreq + " crc=" + crc;
							final double contrib = testFreq * (logNumer - logDenom);
							scores[ex] += (float) contrib;
						}
						else {
							final double logNumer = Math.log(trainFreq + alidstone);
							final double logDenom = Math.log(exToSumFreq[ex] + alidstone * leafVocabSize);
							assert logNumer <= logDenom : crc;
							final double contrib = testFreq * (logNumer - logDenom);
							scores[ex] += (float) contrib;
						}
					}
				}
				// check accuracy
				boolean isCorrect = true;
				for (int sx = 0; sx < scores.length; ++sx) {
					if (sx != gtex && scores[sx] >= scores[gtex]) {
						isCorrect = false;
						break;
					}
				}
				if (isCorrect) {
					++nCorrectInst;
				}
			}
			catch (EOFException eofx) {
				break;
			}
		}
		leafDis.close();
		logger.debug("L" + aleaf + " " + nInst + " " + nCorrectInst/nInst);
		macroAvgAccuracy.add(nCorrectInst/nInst);
		final BayesLidstoneResult blr = new BayesLidstoneResult();
		blr.aleaf = aleaf;
		blr.alidstone = alidstone;
		blr.accuracy = nCorrectInst/nInst;
		blr.instances = nInst;
		blr.store(accuracyDos);
	}
}
