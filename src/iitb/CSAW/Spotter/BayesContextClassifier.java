package iitb.CSAW.Spotter;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.LongIntInt;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Naive Bayes entity disambiguator for annotation.
 * Not thread-safe, use one instance for each thread.
 * @author soumen  
 * @author sasik
 * @since 2011/06/19 sasik Added support for {@link ContextFeatureVector}
 * based classification.
 * @since 2011/07/07 soumen Performance improvements.
 */
public class BayesContextClassifier extends BayesContextBase {
	enum MapType { clef2, clfe3, sh4 };
	final MapType mapType;
	
	final LeafEntityFeatureMaps lefm2;
	final LeafFeatureEntityMaps lfem3;
	final LeafFeatureEntityMaps.LfemReader lfemr3;
	final LfemSignedHashMap sh4;
	
	final Monitor emptyVocabMon = MonitorFactory.getMonitor(getClass().getCanonicalName() + ".emptyVocabMon", null);
	
	public static class Auto {
		protected final LongIntInt key = new LongIntInt(), val = new LongIntInt();
		protected final ContextFeatureVector cfv = new ContextFeatureVector();
		protected final TIntIntHashMap entToFreq = new TIntIntHashMap();
	}
	
	/**
	 * @param conf
	 * @param refTcr token counts collected over reference corpus
	 * @throws Exception
	 */
	BayesContextClassifier(Config conf, TokenCountsReader refTcr) throws Exception {
		super(conf, refTcr);
		final String mapTypeKey = MapType.class.getCanonicalName();
		mapType = MapType.valueOf(conf.getString(mapTypeKey));

		switch (mapType) {
		case clef2:
			// compact, leaf ent feat
			lefm2 = new LeafEntityFeatureMaps(conf);
			lefm2.openRead();
			lfem3 = null;
			lfemr3 = null;
			sh4 = null;
			break;
			
		case clfe3:
			// compact, leaf feat ent
			lfem3 = new LeafFeatureEntityMaps(conf);
			lfem3.openRead();
			lfemr3 = lfem3.openRead();
			lefm2 = null;
			sh4 = null;
			break;
			
		case sh4:
			lefm2 = null;
			lfem3 = null;
			lfemr3 = null;
			sh4 = new LfemSignedHashMap(conf);
			break;
			
		default:
			lefm2 = null;
			lfem3 = null;
			lfemr3 = null;
			sh4 = null;
		}
		logger.info("Finished loading " + getClass().getSimpleName() + ":" + mapType + ".");
	}

	int numTrainingContexts(int trieLeafNodeId) {
		if (lfem3 != null) {
			return lfem3.leaf__nContexts[trieLeafNodeId];
		}
		else if (lefm2 != null){
			return lefm2.leaf__nContexts[trieLeafNodeId];
		}
		else {
			return sh4.leaf__nContexts[trieLeafNodeId];
		}
	}

	public void classifyContext(Auto auto, ContextRecordCompact crc, IntList entIds, boolean loo, TIntFloatHashMap outScores) throws IOException {
		switch (mapType) {
		case clef2:
			if (loo) {
				throw new IllegalArgumentException("LOO not supported by " + MapType.clef2);
			}
			classifyContextUsingLeafEntFeat(auto, crc, entIds, outScores);
			break;
		case clfe3:
			classifyContextUsingLeafFeatEnt(auto, crc, entIds, loo, outScores);
			break;
		case sh4:
			if (loo) {
				throw new IllegalArgumentException("LOO not supported by " + MapType.clef2);
			}			
			classifyContextUsingSignedHash(auto, crc, entIds, outScores);
			break;
		}
	}
	
	/**
	 * @param leaf Leaf Id
	 * @param candEntIds set of cand entities. 
	 * @param crc The ContextRecordCompact for which outscores are required.
	 * @param outScores Output scores. 
	 * @throws IOException 
	 */
	void classifyContextUsingLeafEntFeat(Auto auto, final ContextRecordCompact crc, IntList entIds, final TIntFloatHashMap outScores) throws IOException {
		assert mapType == MapType.clef2 && lefm2 != null : MapType.class + " = " + mapType;
		assert (crc.trieLeaf >= 0) && (crc.trieLeaf < trie.getNumLeaves()): "trieLeafId: " + crc.trieLeaf + " out of bounds.";	
		outScores.clear();
		ContextFeatureVector.makeCountFeatureVector(crc, refTcr, auto.cfv);
		final int vocabSize = lefm2.leaf__vocabSize[crc.trieLeaf];
		if(vocabSize <= 0) {
			emptyVocabMon.add(1);
			logger.debug("leaf=" + crc.trieLeaf + " has no vocabulary.");
			return; //without giving any scores. 
		}
		
		final float featLidstone = lefm2.leafLidstoneParameter != null ? lefm2.leafLidstoneParameter[crc.trieLeaf] : 1f;

		lefm2.initPriors(auto, crc.trieLeaf, entIds, outScores);
		
		for(int ent : entIds) {
			auto.key.write(crc.trieLeaf, ent);
			float sumFreq = 0;
			if(lefm2.leaf_ent__nContexts_sumFreq.containsKey(auto.key.lv)) {
				auto.val.write(lefm2.leaf_ent__nContexts_sumFreq.get(auto.key.lv));
				sumFreq = auto.val.iv0;
			}
			for(TIntFloatIterator itr = auto.cfv.iterator(); itr.hasNext();) {
				itr.advance();
				final int feat = itr.key();
				final float count = itr.value();
				final int entFeatFreq = lefm2.getCount(crc.trieLeaf, ent, feat);
				final float numer = entFeatFreq + featLidstone;
				final float denom = sumFreq + featLidstone * vocabSize;
				assert numer > 0 && denom > 0 : "leaf=" + crc.trieLeaf + " ent=" + ent + " feat" + feat + " cnt=" + count + " sumFreq=" + sumFreq + " lidstoneParam=" + featLidstone;
				if(numer > 0 && denom > 0) {
					final float contrib = count * (float)(Math.log(numer) - Math.log(denom));
					assert contrib <= 0 : "ACTUAL: leaf=" + crc.trieLeaf + " ent=" + ent + " feat=" + feat + " entFeatToFreq=" + entFeatFreq+ " sumFreq=" + sumFreq+ " lidstone=" + featLidstone + " contrib=" + contrib;
					outScores.adjustOrPutValue(ent, contrib, contrib);					
				}
			}
		}
	}
	
	void classifyContextUsingLeafFeatEnt(Auto auto, final ContextRecordCompact crc, IntList entIds, boolean loo, final TIntFloatHashMap outScores) throws IOException {
		assert mapType == MapType.clfe3 && lfem3 != null : MapType.class + " = " + mapType;
		ContextFeatureVector.makeCountFeatureVector(crc, refTcr, auto.cfv);
		final int vocabSize = lfem3.leaf__vocabSize[crc.trieLeaf];
		assert vocabSize>=0 : "leaf=" + crc.trieLeaf + " vocabsize=" + vocabSize + " invalid.";
		if(vocabSize == 0) {
			emptyVocabMon.add(1);
			logger.debug("leaf=" + crc.trieLeaf + " has no vocabulary.");
			return; //without giving any scores. 
		}
		final float featLidstone = lfem3.leafLidstoneParameter == null ? 1f : lfem3.leafLidstoneParameter[crc.trieLeaf];
		outScores.clear();
		lfem3.initPriors(auto, crc.trieLeaf, entIds, outScores);
		for (TIntFloatIterator cfvx = auto.cfv.iterator(); cfvx.hasNext(); ) {
			cfvx.advance();
			final int feat = cfvx.key();
			float testFreq = cfvx.value();
			lfem3.getEntFreq2(lfemr3, crc.trieLeaf, feat, auto.entToFreq);
			final boolean featNotFound = auto.entToFreq.isEmpty();
			for (final int ent : entIds) {
				auto.key.write(crc.trieLeaf, ent);
				final float entSumFreq;
				// Doing this inside the feature loop is slightly inefficient but building up
				// a ent -> sumFreq map and then looking that up isn't likely to be much faster.
				if(lfem3.leaf_ent__nContexts_sumFreq.containsKey(auto.key.lv)) {
					auto.val.write(lfem3.leaf_ent__nContexts_sumFreq.get(auto.key.lv));
					entSumFreq = auto.val.iv0;
				}
				else {
					entSumFreq = 0;
				}
				final float trainFreq = featNotFound? 0 : auto.entToFreq.get(ent); // will get 0 if missing
				final boolean applyLoo = loo && ent == crc.entId;
				final float looTrainFreq = trainFreq - (applyLoo? testFreq : 0);
				final float looEntSumFreq = entSumFreq - (applyLoo? testFreq : 0);
				if (applyLoo && (looTrainFreq < 0 || looEntSumFreq < 0)) {
					throw new IllegalStateException("Bad L-O-O crc=" + crc + " feat=" + feat + " ent=" + ent + " testFreq=" + testFreq + " trainFreq=" + trainFreq + " entSumFreq=" + entSumFreq);
				}
				final float numer = looTrainFreq + featLidstone;
				final float denom = looEntSumFreq + featLidstone * vocabSize;
				assert 0 < numer && numer <= denom : "L" + crc.trieLeaf + " F" + feat + " E" + ent + " num=" + numer + " den=" + denom;
				final float contrib = testFreq * (float) (Math.log(numer) - Math.log(denom));
				outScores.adjustOrPutValue(ent, contrib, contrib);
			}
		}
	}

	/**
	 * {@link LfemSignedHashMap} records log of smoothed probabilities directly so
	 * {@link #classifyContextUsingLeafEntFeat(Auto, ContextRecordCompact, IntList, TIntFloatHashMap)}
	 * or {@link #classifyContextUsingLeafFeatEnt(Auto, ContextRecordCompact, IntList, boolean, TIntFloatHashMap)}
	 * cannot be used directly. Also, loocv cannot be supported.
	 * @param auto
	 * @param crc
	 * @param entIds
	 * @param outScores
	 */
	private void classifyContextUsingSignedHash(Auto auto, ContextRecordCompact crc, IntList entIds, TIntFloatHashMap outScores) {
		assert mapType == MapType.sh4 && sh4 != null : MapType.class + " = " + mapType;
		ContextFeatureVector.makeCountFeatureVector(crc, refTcr, auto.cfv);
		outScores.clear();
		sh4.initPriors(auto, crc.trieLeaf, entIds, outScores);
		for (TIntFloatIterator cfvx = auto.cfv.iterator(); cfvx.hasNext(); ) {
			cfvx.advance();
			final int feat = cfvx.key();
			float testFreq = cfvx.value();
			for (final int ent : entIds) {
				float logParam = sh4.get(crc.trieLeaf, feat, ent);
				outScores.adjustOrPutValue(ent, testFreq * logParam, testFreq * logParam);
			}
		}
	}
}
