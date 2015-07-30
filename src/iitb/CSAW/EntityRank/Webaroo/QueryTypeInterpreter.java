package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TIntDoubleHashMap;
import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;
import gnu.trove.TObjectLongHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.EntityRank.InexTrec.QueryWithAnswers;
import iitb.CSAW.EntityRank.InexTrec.TelegraphQuery;
import iitb.CSAW.EntityRank.Webaroo.SketchySnippet.QEN;
import iitb.CSAW.EntityRank.Webaroo.SketchySnippet.QEWC;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;
import cern.colt.list.BooleanArrayList;

public class QueryTypeInterpreter {
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		QueryTypeInterpreter qti = new QueryTypeInterpreter(conf, new File(args[2]), new File(args[3]));
		qti.loadAggregateQenMap();
		qti.collectQueryStemsFromTelegraphicQueries();
		qti.scanQewcScoreEntities();
	}

	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final File ssDir, traceDir;
	final HashMap<String, String[]> queryIdToWords = new HashMap<String, String[]>();
	final HashMap<String, TIntIntHashMap> queryIdToEntToNumSnip = new HashMap<String, TIntIntHashMap>();
	final TObjectLongHashMap<String> queryIdToNumSnip = new TObjectLongHashMap<String>();
	final ACatalog catalog;
	final TermProcessor termProcessor;
	final TypeLanguageModel tlm;
	
	// TODO move some of these into config file
	/** &delta; in notes */
	final double priorProbHint = 0.2;
	final double tlmSmoother = 0.15;
	final double snipLmSmoother = 0.01;
	final double prTypeGivenEntitySmoother = 0.05;
	
	final int clipRankCutOff = 10;
	final HashMap<String, QueryWithAnswers> qwas = new HashMap<String, QueryWithAnswers>();
	final HashMap<String, RootQuery> teleqs;
	final TIntFloatHashMap queryAtypeCounts = new TIntFloatHashMap();
	
	final String[] traceQueries = new String[] {
			"INEX2009_QA_213", "INEX2007_11", "INEX2007_21"
	};

	QueryTypeInterpreter(Config conf, File ssDir, File traceDir) throws Exception {
		this.conf = conf;
		this.ssDir = ssDir;
		this.traceDir = traceDir;
		this.catalog = ACatalog.construct(conf);
		tlm = new TypeLanguageModel(conf, tlmSmoother);
		termProcessor = DefaultTermProcessor.construct(conf);
		QueryWithAnswers.loadFromMsExcel(conf, conf.getInt(PropertyKeys.windowKey), qwas);
		teleqs = TelegraphQuery.load(new File(conf.getString(TelegraphQuery.telegraphXlsKey)));
		collectQueryAtypeHistogram();
	}
	
	private void collectQueryAtypeHistogram() {
		queryAtypeCounts.clear();
		for (Map.Entry<String, QueryWithAnswers> qwasx : qwas.entrySet()) {
			QueryWithAnswers qwa = qwasx.getValue();
			String atypeName = qwa.bindingType();
			int atype = catalog.catNameToCatID(atypeName);
			if (atype == 0) {
				logger.warn("Probably buggy id for " + atypeName);
			}
			queryAtypeCounts.adjustOrPutValue(atype, 1, 1);
		}
		logger.info("Collected query atype histogram with " + queryAtypeCounts.size() + " atypes.");
	}
	
	/**
	 * scan qen and store (queryId, ent) --> numSnip in ram
	 * also aggr into queryId --> sumEntNumSnip
	 */
	void loadAggregateQenMap() throws IOException {
		final File qenFile = new File(ssDir, QEN.class.getCanonicalName() + ".dat");
		logger.info("Started loading " + qenFile);
		final InputBitStream qenIbs = new InputBitStream(new FileInputStream(qenFile));
		final QEN qen = new QEN();
		for (;;) {
			try {
				qen.load(qenIbs);
				final String queryId = qen.queryId.toString();
				final TIntIntHashMap entToNumSnip;
				if (queryIdToEntToNumSnip.containsKey(queryId)) {
					entToNumSnip = queryIdToEntToNumSnip.get(queryId);
				}
				else {
					entToNumSnip = new TIntIntHashMap();
					queryIdToEntToNumSnip.put(queryId, entToNumSnip);					
				}
				entToNumSnip.adjustOrPutValue(qen.ent, qen.numSnippets, qen.numSnippets);
				queryIdToNumSnip.adjustOrPutValue(queryId, qen.numSnippets, qen.numSnippets);
			}
			catch (EOFException eofx) {
				break;
			}
		}
		qenIbs.close();
		logger.info("Finished loading " + qenFile);
	}
	
	/**
	 * Collect query stems from {@link TelegraphQuery} {@link #teleqs}.
	 * @throws IOException
	 */
	void collectQueryStemsFromTelegraphicQueries() throws IOException {
		queryIdToWords.clear();
		for (Map.Entry<String, RootQuery> tqx : teleqs.entrySet()) {
			ArrayList<String> qtoks = TelegraphQuery.telegraphQueryToTokenLiterals(tqx.getValue());
			MutableString qstem = new MutableString();
			ArrayList<String> qstems = new ArrayList<String>();
			for (String qtok : qtoks) {
				qstem.replace(qtok);
				if (termProcessor.processTerm(qstem)) {
					qstems.add(qstem.toString());
				}
			}
			queryIdToWords.put(tqx.getKey(), qstems.toArray(new String[]{}));
		}
	}

	/**
	 * We could always collect the query text from {@link TelegraphQuery}, but
	 * that may get out of whack wrt what is matched owing to
	 * {@link TermProcessor} etc. So we tap it "at the source".
	 * Query words that did not match for any candidate entity are ignored
	 * --- is this correct?
	 */
	@Deprecated
	void collectQueryStemsFromQewc() throws IOException {
		queryIdToWords.clear();
		final File qewcFile = new File(ssDir, QEWC.class.getCanonicalName() + ".dat");
		final InputBitStream qewcIbs = new InputBitStream(new FileInputStream(qewcFile));
		final QEWC qewc = new QEWC();
		final MutableString queryWordStem = new MutableString();
		final HashSet<String> queryStems = new HashSet<String>();
		String prevQueryId = null;
		for (;;) {
			try {
				qewc.load(qewcIbs);
				if (prevQueryId != null && !qewc.queryId.equals(prevQueryId)) {
					logger.info(prevQueryId + " --> " + queryStems + " ... " + TelegraphQuery.telegraphQueryToTokenLiterals(teleqs.get(prevQueryId)));
					queryIdToWords.put(prevQueryId, queryStems.toArray(new String[]{}));
					queryStems.clear();
				}
				for (TObjectIntIterator<String> qewcx = qewc.queryWordCount.iterator(); qewcx.hasNext(); ) {
					qewcx.advance();
					queryWordStem.replace(qewcx.key());
					if (termProcessor.processTerm(queryWordStem)) {
						queryStems.add(queryWordStem.toString());
					}
				}
				prevQueryId = qewc.queryId.toString();
			}
			catch (EOFException eofx) {
				break;
			}
		}
		if (prevQueryId != null) {
			if (prevQueryId != null) {
				logger.info(prevQueryId + " L--> " + queryStems + " ... " + TelegraphQuery.telegraphQueryToTokenLiterals(teleqs.get(prevQueryId)));
				queryIdToWords.put(prevQueryId, queryStems.toArray(new String[]{}));
			}
			queryStems.clear();
		}
		qewcIbs.close();
		logger.info("Collected words for " + queryIdToWords.size() + " queries.");
	}
	
	/**
	 * Evaluates candidate entity scores by taking expectation over possible hint words and atypes.
	 */
	void scanQewcScoreEntities() throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		final File qewcFile = new File(ssDir, QEWC.class.getCanonicalName() + ".dat");
		final InputBitStream qewcIbs = new InputBitStream(new FileInputStream(qewcFile));
		// will map ent scores for a run of queryId, then sort and output
		final TIntDoubleHashMap entLogProbs = new TIntDoubleHashMap();
		int qcntGen = 0; // used in ranking assessment code
		/*
		 * TODO qwas.size() below may be an overestimate that affects perceived accuracy. 
		 */
		final Accuracy.All summary = new Accuracy.All(qwas.size(), clipRankCutOff); 
		String prevQueryId = null;
		final QEWC qewc = new QEWC();
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = queryIdToWords.size();
		pl.start();
		for (;;) { // 
			try {
				qewc.load(qewcIbs);
				final String entName = catalog.entIDToEntName(qewc.ent);
				final String queryId = qewc.queryId.toString();
				if (prevQueryId != null && !prevQueryId.equals(queryId)) {
					evaluateQueryPerformance(++qcntGen, prevQueryId, entLogProbs, qwas.get(prevQueryId), summary);
					entLogProbs.clear();
					pl.update();
				}
				stemUpdateInPlace(qewc.queryWordCount);
				final String[] stemmedQueryWords = queryIdToWords.get(queryId); 
				TIntHashSet catIdSet = new TIntHashSet();
				catalog.catsReachableFromEnt(qewc.ent, catIdSet);
				final int[] cats = catIdSet.toArray();
				logger.trace(queryId + " " + (1<<stemmedQueryWords.length) * cats.length);
				if (stemmedQueryWords.length == 0) {
					logger.warn("Empty query match, omitting " + queryId);
					continue;
				}
				if (entLogProbs.containsKey(qewc.ent)) {
					throw new IllegalStateException();
				}
				entLogProbs.put(qewc.ent, 0);
				final double logProbEmptySnippet = getLogProbEmptySnippet(qewc);
				final double[] logOddsSnippet = new double[stemmedQueryWords.length];
				for (int lx = 0; lx < logOddsSnippet.length; ++lx) {
					logOddsSnippet[lx] = getLogOddsSnippet(stemmedQueryWords[lx], qewc); 
				}
				final double priorProbOfEnt = getPriorProbOfEnt(queryId, qewc.ent);
				// decoupled hint+type iterator into two nested loops, to reduce computation
				for (final int cat : cats) { // atype loop
					@SuppressWarnings("unused")
					final String atypeName = catalog.catIDToCatName(cat);
					final double logProbEmptyType = tlm.logProbEmptyGivenType(cat);
					final double[] logOddsType = new double[stemmedQueryWords.length];
					for (int lx = 0; lx < logOddsType.length; ++lx) {
						logOddsType[lx] = tlm.logOddsWordGivenType(stemmedQueryWords[lx], cat);
					}
					for (SlidingHintIterator hi = new SlidingHintIterator(stemmedQueryWords); hi.hasNext(); ) { // hint loop
						hi.advance();
						final boolean[] hintFlags = new boolean[stemmedQueryWords.length];
						hi.get(hintFlags);
						// queryStems hintFlags logProbEmptySnippet logOddsSnippet logProbEmptyType logOddsType
						final double probOfAtypeGivenEnt = getProbOfAtypeGivenEnt(cat, cats, qewc.ent);
						final double priorProbOfHintFlags = getPriorProbOfHintFlags(stemmedQueryWords, hintFlags);
						double entLogScore = 0;
						entLogScore += Math.log(priorProbOfEnt);
						entLogScore += Math.log(probOfAtypeGivenEnt);
						entLogScore += Math.log(priorProbOfHintFlags);
						entLogScore += logProbEmptyType;
						entLogScore += logProbEmptySnippet;
						for (int lx = 0; lx < hintFlags.length; ++lx) {
							if (hintFlags[lx]) { // type LM
								entLogScore += logOddsType[lx];
							}
							else { // snippet LM
								entLogScore += logOddsSnippet[lx];
							}
						}
						final double entScoreInc = Math.exp(entLogScore);
						entLogProbs.adjustOrPutValue(qewc.ent, entScoreInc, entScoreInc);
						tracePrint(stemmedQueryWords, hintFlags, cat, qewc, entScoreInc);
					} // hint loop
				} // atype loop
				prevQueryId = queryId;
			}
			catch (EOFException eofx) {
				break;
			}
		}
		if (prevQueryId != null) {
			evaluateQueryPerformance(++qcntGen, prevQueryId, entLogProbs, qwas.get(prevQueryId), summary);
			entLogProbs.clear();
			pl.update();
		}
		pl.stop();
		pl.done();
		qewcIbs.close();
		summary.average();
		summary.printText(System.out);
	}
	
	private void tracePrint(String[] stemmedQueryWords, boolean[] hintFlags, int cat, QEWC qewc, double entScoreInc) {
		StringBuilder sb = new StringBuilder();
		sb.append(qewc.queryId + " [");
		for (int hx = 0; hx < hintFlags.length; ++hx) {
			sb.append(stemmedQueryWords[hx] + (hintFlags[hx]? "* " : " "));
		}
		sb.append("] cat=");
		sb.append(new ReferenceArrayList<String>(tlm.catToWords.get(cat)) + " ");
		sb.append(" ent=" + catalog.entIDToEntName(qewc.ent) + " ");
		sb.append(" snippetLM=" + qewc.queryWordCount);
		sb.append(" score=" + entScoreInc);
		logger.trace(sb);
	}

	/**
	 * {@link QEWC#queryWordCount} holds unstemmed words, stem and
	 * {@link TermProcessor} them, omit if rejected by {@link TermProcessor}. 
	 * @param queryWordCount
	 */
	private void stemUpdateInPlace(TObjectIntHashMap<String> queryWordCount) {
		TObjectIntHashMap<String> stemCounts = new TObjectIntHashMap<String>();
		MutableString word = new MutableString();
		for (TObjectIntIterator<String> qwcx = queryWordCount.iterator(); qwcx.hasNext(); ) {
			qwcx.advance();
			word.replace(qwcx.key());
			if (termProcessor.processTerm(word)) {
				stemCounts.put(word.toString(), qwcx.value());
			}
		}
		queryWordCount.clear();
		queryWordCount.putAll(stemCounts);
	}

	/**
	 * @param queryId
	 * @param ent
	 * @return unsmoothed ratio of support of ent to total support
	 */
	private double getPriorProbOfEnt(String queryId, int ent) {
		assert queryIdToNumSnip.containsKey(queryId);
		return fromQueryIdEntGetNumSnip(queryId, ent) / queryIdToNumSnip.get(queryId);
	}
	
	/**
	 * @param queryWords ignored, might improve to look at the words
	 * @param hintFlags
	 * @return product of Bernoulli trial probabilities
	 */
	private double getPriorProbOfHintFlags(String[] queryWords, boolean[] hintFlags) {
		double ans = 1;
		for (int hx = 0; hx < hintFlags.length; ++hx) {
			ans *= (hintFlags[hx]? priorProbHint : (1d - priorProbHint));
		}
		return ans;
	}
	
	/**
	 * @param cat must be ancestor of ent
	 * @param cats must contain cat
	 * @param ent
	 * @return prior probability of atype given entity 
	 */
	private double getProbOfAtypeGivenEnt(int cat, int[] cats, int ent) {
//		return 1d / cats.length; .... uniform
		double rawNumer = queryAtypeCounts.get(cat), rawDenom = 0;
		for (int acat : cats) {
			rawDenom += queryAtypeCounts.get(acat);
		}
		return (rawNumer + prTypeGivenEntitySmoother) / (rawDenom + prTypeGivenEntitySmoother * rawDenom);
	}

	private double fromQueryIdEntGetNumSnip(String queryId, int ent) {
		if (!queryIdToEntToNumSnip.containsKey(queryId)) { return 0; }
		final TIntIntHashMap entToNumSnip = queryIdToEntToNumSnip.get(queryId);
		return entToNumSnip.get(ent);
	}
	
	private double getLogProbEmptySnippet(QEWC qewc) {
		// FIXME use corpus occurrence frequency
		final double numSnip = fromQueryIdEntGetNumSnip(qewc.queryId.toString(), qewc.ent);
		double ans = 0;
		for (TObjectIntIterator<String> qewcx = qewc.queryWordCount.iterator(); qewcx.hasNext(); ) {
			qewcx.advance();
			final double prob = (snipLmSmoother + qewcx.value()) / (2d * snipLmSmoother + numSnip);
			ans += Math.log(1d - prob);
		}		
		return ans;
	}

	private double getLogOddsSnippet(String stem, QEWC qewc) {
		// FIXME use corpus occurrence frequency
		final double numSnip = fromQueryIdEntGetNumSnip(qewc.queryId.toString(), qewc.ent);
		@SuppressWarnings("unused")
		final boolean exists = qewc.queryWordCount.containsKey(stem);
		final double count = qewc.queryWordCount.get(stem);
		final double prob =  (snipLmSmoother + count) / (2d * snipLmSmoother + numSnip);
		assert 0 < prob && prob < 1;
		return Math.log(prob / (1d - prob));
	}
	
	private void evaluateQueryPerformance(int qnum, String queryId, final TIntDoubleHashMap entScores, QueryWithAnswers qwa, Accuracy.All summary) {
		if (qwa == null) {
			logger.warn("Query " + queryId + " has no ground truth, skipping.");
			return;
		}
		// No need for Units class ---
		final int[] ents = entScores.keys();
		assert ents.length == entScores.size();
		Sorting.quickSort(ents, 0, ents.length, new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final double v1 = entScores.get(o1), v2 = entScores.get(o2);
				if (v1 > v2) { return -1; }
				if (v1 < v2) { return 1; }
				return 0;
			}
		});
		// paranoid
		for (int ex = 1; ex < ents.length; ++ex) {
			assert entScores.get(ents[ex-1]) >= entScores.get(ents[ex]);
		}
		final TIntHashSet posEnts = new TIntHashSet();
		for (String posEntName : qwa.posEntNames) {
			final int ent = catalog.entNameToEntID(posEntName);
			if (ent >= 0) {
				posEnts.add(ent);
			}
		}
		boolean[] entLabelArray = new boolean[ents.length];
		int numEntTrue = 0, firstGood = Integer.MAX_VALUE;
		for (int ex = 0; ex < ents.length; ++ex) {
			if (posEnts.contains(ents[ex])) {
				entLabelArray[ex] = true;
				++numEntTrue;
				firstGood = Math.min(firstGood, ex);
			}
		}
		logger.info(queryId + " numEntScored=" + entScores.size() + " numEntTrue=" + numEntTrue + "/" + posEnts.size() + " firstGood=" + (firstGood == Integer.MAX_VALUE? "inf" : firstGood) + " of " + entLabelArray.length);
		// number of pos ents for this query consider only those found in the catalog
		RankEvaluator rev = new RankEvaluator(entLabelArray, posEnts.size());
		for (int testClipRank = 1; testClipRank <= clipRankCutOff; ++testClipRank) {
			final double dummySigma2 = 0;
			summary.update(testClipRank, dummySigma2, rev, qnum);
		}
	}
	
	/**
	 * One among many possible iterators through the hint space.
	 * This one assumes any nonempty subset of query words can be the hint.
	 * The entire set of query words is allowed to be the hint.
	 * Other iterators may only permit short spans or small subsets.
	 */
	private static class NonemptyHintIterator {
		final String[] queryWords;
		int hintGen;
		NonemptyHintIterator(String[] queryWords) {
			this.queryWords = queryWords;
			if (this.queryWords.length == 0) {
				throw new IllegalArgumentException();
			}
			hintGen = 1;
		}
		boolean hasNext() {
			return hintGen < (1<<queryWords.length) - 1;
		}
		boolean advance() {
			if (!hasNext()) { return false; }
			++hintGen;
			return true;
		}
		void get(boolean[] hintFlags) {
			Arrays.fill(hintFlags, false);
			for (int hintMask = hintGen, pos = 0; hintMask != 0; ++pos) {
				if ((hintMask & 1) != 0) {
					hintFlags[pos] = true;
				}
				hintMask >>>= 1;
			}
		}
	}

	/**
	 * Sliding window of at most so many words in the query can be hint.
	 */
	private static class SlidingHintIterator {
		final int maxSpan = 2;
		final String[] queryWords;
		int from=0, to=0;
		SlidingHintIterator(String[] queryWords) {
			this.queryWords = queryWords;
			from = to = 0;
		}
		boolean hasNext() {
			if (to - from < maxSpan && to < queryWords.length) { return true; }
			if (from + maxSpan < queryWords.length) { return true; }
			return false;
		}
		boolean advance() {
			if (to - from < maxSpan && to < queryWords.length) {
				++to;
				return true;
			}
			if (from + maxSpan < queryWords.length) {
				++from;
				to = from + 1;
				return true;
			}
			return false;
		}
		void get(boolean[] hintFlags) {
			Arrays.fill(hintFlags, false);
			for (int hx = from; hx < to; ++hx) {
				hintFlags[hx] = true;
			}
		}
	}
	
	public static void main3(String[] args) throws Exception {
		String[] query = new String[]{ "mary", "had", "a", "little", "lamb"};
		SlidingHintIterator shi = new SlidingHintIterator(query);
		boolean[] hints = new boolean[query.length];
		while (shi.hasNext()) {
			shi.advance();
			shi.get(hints);
			System.out.println(new BooleanArrayList(hints));
		}
	}
}
