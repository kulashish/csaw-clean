package iitb.CSAW.Spotter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;

import gnu.trove.TIntFloatHashMap;
import iitb.CSAW.Spotter.BayesContextClassifier.Auto;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IntIntToIntIntHashMap;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.io.BinIO;

/**
 * Base class for implementations of l-f-e maps for naive Bayes disambiguators.
 * Provides maps
 * from leaf to number of contexts,
 * from leaf to feature vocabulary size,
 * from (leaf, ent) to number of contexts and sum of feature frequencies.
 * Should share one instance across threads.
 * @author soumen
 */
public abstract class ALfeMap {
	final Config conf;
	final File contextBaseDir, lfemBaseDir;
	final Logger logger = Logger.getLogger(getClass());
	final MentionTrie trie;

	/*
	 * Key fields are separated from each other by a single _
	 * key and value fields are separated by a double __
	 * value fields are separated from each other by a single _
	 */
	final int hashOverDoseFactor = 3; // depends on typical num ents per leaf
	int[] leaf__nContexts;
	int[] leaf__vocabSize;
	IntIntToIntIntHashMap leaf_ent__nContexts_sumFreq;
	float[] leafLidstoneParameter;
	
	static final String leafNumCtxName = "LeafNumCtx.dat";
	static final String leafEntToNumCtxSumFreqName = "LeafEntNumCtxSumFreq.dat";
	static final String leafVocabSizeName = "LeafVocabSize.dat";
	static final String leafLidstoneParameterName = "LeafLidstoneParameter.dat";
	
	/** Default is Laplace correction for label priors. */
	static final float entPriorLidstone = 1;
	
	ALfeMap(Config conf, boolean doWrite) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		this.conf = conf;
		contextBaseDir = new File(conf.getString(PropertyKeys.contextBaseDirKey2));
		lfemBaseDir = new File(conf.getString(getClass().getSimpleName() + ".dir"));
		trie = MentionTrie.getInstance(conf);
		if (doWrite) {
			leaf__nContexts = new int[trie.getNumLeaves()];
			leaf__vocabSize = new int[trie.getNumLeaves()];
			leaf_ent__nContexts_sumFreq = new IntIntToIntIntHashMap(hashOverDoseFactor * trie.getNumLeaves());
			leafLidstoneParameter = null;
		}
		else {
			// common between leaf,ent,feat and leaf,feat,ent
			if (leaf_ent__nContexts_sumFreq == null) {
				File lens = new File(lfemBaseDir, leafEntToNumCtxSumFreqName);
				leaf_ent__nContexts_sumFreq = (IntIntToIntIntHashMap) BinIO.loadObject(lens);
			}
			if (leaf__nContexts == null) {
				leaf__nContexts = BinIO.loadInts(new File(lfemBaseDir, leafNumCtxName));
			}
			if (leaf__vocabSize == null) {
				leaf__vocabSize = BinIO.loadInts(new File(lfemBaseDir, leafVocabSizeName));
			}
			if (leafLidstoneParameter == null) {
				final File lidstoneFile = new File(contextBaseDir, leafLidstoneParameterName);
				if (lidstoneFile.canRead()) {
					logger.info("Loading " + lidstoneFile);
					leafLidstoneParameter = BinIO.loadFloats(lidstoneFile);
					assert leafLidstoneParameter.length == trie.getNumLeaves() : "LeafLidstoneParameter size: " + leafLidstoneParameter.length + ", orig: " + trie.getNumLeaves();
				}
				else {
					logger.warn("Cannot read " + lidstoneFile);
					leafLidstoneParameter = null;
				}
			}
			checkSanity();
		}
	}
	
	private void checkSanity() {
		final int noOfLeaves = trie.getNumLeaves();
		assert leaf_ent__nContexts_sumFreq != null : "LeafEntToNumCtxSumFreq is null.";
		assert leaf_ent__nContexts_sumFreq.size() > 0 : "LeafEntToNumCtxSumFreq is empty.";
		assert leaf__nContexts != null : "LeafToNumCtx is null";
		assert leaf__nContexts.length > 0 : "LeafNumCtx is empty.";
		assert leaf__vocabSize != null : "LeafVocabSize is null";
		assert leaf__vocabSize.length > 0 : "LeafVocabSize is empty.";
		assert leaf__nContexts.length == noOfLeaves : "LeafToNumCtx size : " + leaf__nContexts.length + ", orig: " + noOfLeaves;
		assert leaf__vocabSize.length == noOfLeaves : "LeafVocabSize size: " + leaf__vocabSize.length + ", orig: " + noOfLeaves;
		int noVocabLeaves = 0, tempVocabSize = 0;
		for(int i=0, n=leaf__vocabSize.length; i<n; ++i) {
			tempVocabSize = leaf__vocabSize[i];
			assert tempVocabSize > -1 : "leaf=" + i + " vocab=" + tempVocabSize + ", should be >=0";
			if(tempVocabSize == 0) {
//				logger.warn("leaf=" + i + " vocab=0");
				++noVocabLeaves;
			}
		}		
		logger.warn(noVocabLeaves + " of " + noOfLeaves + " leaves have no vocabulary.");		
	}
	
	void store() throws IOException {
		BinIO.storeObject(leaf_ent__nContexts_sumFreq, new File(lfemBaseDir, leafEntToNumCtxSumFreqName));
		BinIO.storeInts(leaf__nContexts, new File(lfemBaseDir, leafNumCtxName));
		BinIO.storeInts(leaf__vocabSize, new File(lfemBaseDir, leafVocabSizeName));
		logger.debug("leaf,ent=" + leaf_ent__nContexts_sumFreq.size());
	}
	
	void initPriors(Auto auto, int leaf, IntList entIds, TIntFloatHashMap scores) {
		final int numCtx = leaf__nContexts[leaf];
		final int numCandEnts = entIds.size();	
		if(numCtx > 0) {
			for(int ent : entIds) {
				int entNumCtx = 0;
				auto.key.write(leaf, ent);
				if(leaf_ent__nContexts_sumFreq.containsKey(auto.key.lv)) {
					auto.val.write(leaf_ent__nContexts_sumFreq.get(auto.key.lv));					
					entNumCtx = auto.val.iv1;
				}
				final float numer = entNumCtx + entPriorLidstone;	 
				final float denom = numCtx + entPriorLidstone * numCandEnts;
				assert numer > 0 && denom > 0 && denom >= numer : "leaf=" + leaf + " ent=" + ent + " entNumCtx" + entNumCtx + " numCtx=" + numCtx + " numCandEnts=" + numCandEnts;				
				if(numer > 0 && denom > 0) {
					scores.put(ent, (float) (Math.log(numer) - Math.log(denom)) );
				} 				
			}			
		}
		else {
			logger.warn("leaf=" + leaf + " has no priors."); 
		}
	}
}
