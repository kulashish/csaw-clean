package iitb.CSAW.Spotter;

import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntHashSet;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.MemoryStatus;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.ConfigurationException;

public class BayesContextTrainer extends ContextBase {
	/**
	 * "Trains" a naive Bayes classifier and saves model files to disk.
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		TokenCountsReader refTcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		BayesContextTrainer bct = new BayesContextTrainer(conf, refTcr);
		bct.runCompact();
	}
	
	final MemoryStatus ms = new MemoryStatus();

	BayesContextTrainer(Config conf, TokenCountsReader refTcr) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, ConfigurationException {
		super(conf, refTcr);
	}
	
	/**
	 * Reads {@link ContextRecordCompact} file and saves stats through
	 * some implementation of L-F-E-map.
	 * @throws Exception
	 */
	void runCompact() throws Exception {
		final LeafFeatureEntityMaps lfem = new LeafFeatureEntityMaps(conf, true);
		lfem.beginIndexing(false);
		final ContextRecordCompact crc = new ContextRecordCompact();
		final ContextFeatureVector cfv = new ContextFeatureVector();
		long nEmptyLeaves = 0, nRecs = 0;
		ProgressLogger pl2 = new ProgressLogger(logger);
		pl2.expectedUpdates = trie.getNumLeaves();
		pl2.displayFreeMemory = true;
		pl2.start("Started leaf scan to store NB stats.");
		for (int leaf = 0; leaf < trie.getNumLeaves(); ++leaf) {
			final DataInputStream leafDis = getLeafStream(leaf);
			if (leafDis == null) {
				++nEmptyLeaves;
				continue;
			}
			lfem.beginLeaf(leaf);
			final TIntHashSet leafFeatureVocab = new TIntHashSet();
			int prevEnt = Integer.MIN_VALUE;
			for (;;) {
				try {
					crc.load(leafDis);
					++nRecs;
					assert crc.trieLeaf == leaf : "Leaf ID should be " + leaf + " but is " + crc.trieLeaf;
					assert prevEnt <= crc.entId : "Ent ID decreased from " + prevEnt + " to " + crc.entId;
					if (prevEnt < crc.entId) {
						if (prevEnt != Integer.MIN_VALUE) {
//							lefm.endLeafEntBlock();
						}
//						lefm.beginLeafEntBlock(leaf, crc.entId);
					}
					++lfem.leaf__nContexts[crc.trieLeaf];
					ContextFeatureVector.makeCountFeatureVector(crc, refTcr, cfv);
					float l1norm = 0;
					for (TIntFloatIterator cfx = cfv.iterator(); cfx.hasNext(); ) {
						cfx.advance();
						l1norm += Math.abs(cfx.value());
//						lefm.accumulate(cfx.key(), (int) cfx.value());
						lfem.accumulate(cfx.key(), cfv.ent, (int) cfx.value());
						if (!leafFeatureVocab.contains(cfx.key())) {
							leafFeatureVocab.add(cfx.key());
						}
					}
					final int sumFreq = Math.round(l1norm);
					lfem.leaf_ent__nContexts_sumFreq.adjustValues(cfv.leaf, cfv.ent, 1, sumFreq, null, null);
					prevEnt = crc.entId;
				}
				catch (EOFException eofx) {
					logger.debug("L" + leaf + " " + eofx);
					break;
				}
			}
			lfem.endLeaf();
			if (prevEnt != Integer.MIN_VALUE) {
//				lefm.endLeafEntBlock();
			}
			leafDis.close();
			pl2.update();
			lfem.leaf__vocabSize[leaf] = leafFeatureVocab.size();
		} // for leaf
		pl2.stop("Finished leaf scan after " + nRecs + " records");
		pl2.done();
//		lefm.endIndexing();
		lfem.endIndexing();
		if (nEmptyLeaves > 0) logger.warn(nEmptyLeaves + " empty leaf blocks");
	}
}
