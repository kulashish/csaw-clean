package iitb.CSAW.Search;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.EntityRank.InexTrec.TelegraphQuery;
import iitb.CSAW.EntityRank.Webaroo.IBitReducible;
import iitb.CSAW.EntityRank.Webaroo.SketchySnippet;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Index.TokenLiteralWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Index.SIP2.Sip2Document;
import iitb.CSAW.Index.SIP2.Sip2IndexReader;
import iitb.CSAW.Index.SIP2.Sip2IndexReader.SipSpanIterator;
import iitb.CSAW.Query.AtomQuery;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.Sort.BitExternalMergeReduce;
import iitb.CSAW.Utils.Sort.BitSortedRunWriter;
import it.unimi.dsi.fastutil.ints.Int2ReferenceMap;
import it.unimi.dsi.fastutil.ints.Int2ReferenceRBTreeMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleMap;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.dsi.mg4j.query.nodes.Term;
import it.unimi.dsi.mg4j.search.AndDocumentIterator;
import it.unimi.dsi.mg4j.search.CachingDocumentIterator;
import it.unimi.dsi.mg4j.search.ConsecutiveDocumentIterator;
import it.unimi.dsi.mg4j.search.DocumentIterator;
import it.unimi.dsi.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.dsi.mg4j.search.IntervalIterator;
import it.unimi.dsi.mg4j.search.OrDocumentIterator;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableDouble;
import org.apache.log4j.Logger;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Uses only the token index created by MG4J to return an iterator
 * over documents that satisfy a wand predicate. Intended to be run on
 * {@link TelegraphQuery}s, not general CSAW queries.
 * Preliminary untested version.
 * @author soumen
 */
public class TokenWandQueryProcessor {
	/**
	 * @param args [0]=config [1]=log [2]=snippetRecordFile
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		TokenWandQueryProcessor wandQp = new TokenWandQueryProcessor(conf);
		wandQp.run(new File(args[2]));
	}
	
	final Config conf;
	final Logger logger = Logger.getLogger(getClass());
	final AStripeManager stripeManager;
	final Index stemIndex;
	final Object2ReferenceOpenHashMap<String,Index> indexMap = new Object2ReferenceOpenHashMap<String,Index>();

	final TermProcessor termProcessor;
	final File sipIndexDiskDir;
	final Sip2IndexReader sipIndex;
	final ACatalog catalog;
	/** 
	 * Token counts over payload corpus, not reference corpus,
	 * because annotation is already done and indexed by now.
	 */
	final TokenCountsReader payloadTcr;
	final int yagoEntityTypeId = 151778;
	final double wandThresholdFraction = .6;
	final Monitor nQueryDocs = MonitorFactory.getMonitor("nQueryDocs", null);
	final Monitor nQueryWandDocs = MonitorFactory.getMonitor("nQueryWandDocs", null);
	final Monitor nQueryIntervals = MonitorFactory.getMonitor("nQueryIntervals", null);
	final Monitor nQuerySurvivingIntervals = MonitorFactory.getMonitor("nQuerySurvivingIntervals", null);

	final int runBytes = 0x7fffffff;
	BitSortedRunWriter<SketchySnippet.QEWC> bsrwQewc = new BitSortedRunWriter<SketchySnippet.QEWC>(SketchySnippet.QEWC.class, runBytes);
	BitSortedRunWriter<SketchySnippet.QEN> bsrwQen = new BitSortedRunWriter<SketchySnippet.QEN>(SketchySnippet.QEN.class, runBytes);
	AtomicInteger runGen = new AtomicInteger();
	
	TokenWandQueryProcessor(Config conf) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, URISyntaxException, ConfigurationException, IOException {
		this.conf = conf;
		stripeManager = AStripeManager.construct(conf);
		final URI tokenIndexUri = stripeManager.tokenIndexDiskDir(stripeManager.myDiskStripe());
		assert tokenIndexUri.getHost().equals(stripeManager.myHostName());
		final File tokenIndexDiskDir = new File(tokenIndexUri.getPath());
		final String stemName = Field.token.toString();
		final String stemBase = (new File(tokenIndexDiskDir, stemName)).toString();
		stemIndex = Index.getInstance(stemBase, true);
		termProcessor = DefaultTermProcessor.construct(conf);
		URI sipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		sipIndexDiskDir = new File(sipIndexDiskUri.getPath());
		sipIndex = new Sip2IndexReader(conf);
		catalog = ACatalog.construct(conf);
		payloadTcr = new TokenCountsReader(tokenIndexUri);
		indexMap.put(ACorpus.Field.token.toString(), stemIndex);
	}
	
	void run(File srDir) throws Exception {
		HashMap<String, RootQuery> tQueries = TelegraphQuery.load(new File(conf.getString(TelegraphQuery.telegraphXlsKey)));
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = tQueries.size();
		pl.logInterval = ProgressLogger.ONE_MINUTE/2L;
		pl.start();
		for (RootQuery tq : tQueries.values()) {
//			if (!Arrays.asList("TREC2000:637", "TREC2002:1875" , "TREC2000:463", "TREC2000:466", "TREC2000:621", "TREC2001:966" ).contains(tq.queryId)) continue;
//			if (!tq.queryId.equals("TREC2002:1700")) continue;
			logger.info(tq);
			if (tq.contexts.size() != 1) throw new IllegalArgumentException(tq.toString());
			runOneQuery(tq, srDir);
			pl.update();
			printStats();
		} // for query
		pl.stop();
		pl.done();
		printStats();
		saveAnotherRunFile(srDir, bsrwQewc, SketchySnippet.QEWC.class);
		saveAnotherRunFile(srDir, bsrwQen, SketchySnippet.QEN.class);
	}
	
	private void printStats() {
		logger.info(nQueryDocs);
		logger.info(nQueryWandDocs);
		logger.info(nQueryIntervals);
		logger.info(nQuerySurvivingIntervals);
	}
	
	void runOneQuery(RootQuery tq, File srDir) throws QueryBuilderVisitorException, IOException, InstantiationException, IllegalAccessException {
		Reference2DoubleOpenHashMap<DocumentIterator> docIterToTermWeight = new Reference2DoubleOpenHashMap<DocumentIterator>();
		Reference2ReferenceOpenHashMap<DocumentIterator, MatcherQuery> docIterToMatcherQuery = new Reference2ReferenceOpenHashMap<DocumentIterator, MatcherQuery>();
		MutableDouble sumOrTermWeight = new MutableDouble();
		DocumentIterator preWandIterator = prepareOneQuery(indexMap, tq, sumOrTermWeight, docIterToMatcherQuery, docIterToTermWeight);
//		assert docIterToMatcherQuery.keySet().equals(docIterToTermWeight.keySet()) : docIterToMatcherQuery.keySet() + " != " + docIterToTermWeight.keySet();
		executeOneQuery(tq, preWandIterator, sumOrTermWeight, docIterToMatcherQuery, docIterToTermWeight, srDir);
	}

	/**
	 * @param indexMap
	 * @param tq
	 * @param sumOrTermWeight will not be correct if words are repeated in the query
	 * @param docIterToMatcherQuery
	 * @param docIterToTermWeight
	 * @return document iterator
	 * @throws QueryBuilderVisitorException
	 * @throws IOException
	 */
	private DocumentIterator prepareOneQuery(Object2ReferenceMap<String, Index> indexMap, RootQuery tq, MutableDouble sumOrTermWeight, Reference2ReferenceMap<DocumentIterator, MatcherQuery> docIterToMatcherQuery, Reference2DoubleMap<DocumentIterator> docIterToTermWeight) throws QueryBuilderVisitorException, IOException {
		sumOrTermWeight.setValue(0);
		DocumentIteratorBuilderVisitor dibv = new DocumentIteratorBuilderVisitor( indexMap, stemIndex, Integer.MAX_VALUE);
		ReferenceArrayList<DocumentIterator> andClauses = new ReferenceArrayList<DocumentIterator>(), orClauses = new ReferenceArrayList<DocumentIterator>();
		ContextQuery tcq = tq.contexts.top();
		for (MatcherQuery mq : tcq.matchers) {
			if (mq instanceof TokenLiteralQuery) {
				final MutableString tokenStem = new MutableString(((TokenLiteralQuery) mq).tokenText);
				if (!termProcessor.processTerm(tokenStem)) {
					logger.warn(tokenStem.toString() + " cannot be termProcessed.");
					continue;
				}
				dibv.prepare();
				DocumentIterator stemIterator = new CachingDocumentIterator(new Term(tokenStem).accept(dibv));
				double logIdf = payloadTcr.logIdf(tokenStem, false);
				if (!Double.isInfinite(logIdf)) {
					docIterToTermWeight.put(stemIterator, logIdf);
					docIterToMatcherQuery.put(stemIterator, mq);
					if (mq.exist == Exist.may) {
						sumOrTermWeight.add(logIdf);
					}
				}
				else {
					logger.info(tokenStem + " not in index");
				}
				(mq.exist == Exist.may? orClauses : andClauses).add(stemIterator);
			}
			else if (mq instanceof PhraseQuery) {
				PhraseQuery pq = (PhraseQuery) mq;
				ReferenceArrayList<DocumentIterator> termIterators = new ReferenceArrayList<DocumentIterator>();
				for (AtomQuery aq : pq.atoms) {
					if (aq instanceof TokenLiteralQuery) {
						final MutableString tokenStem = new MutableString(((TokenLiteralQuery) aq).tokenText);
						if (!termProcessor.processTerm(tokenStem)) {
							logger.warn(tokenStem.toString() + " cannot be termProcessed.");
							continue;
						}
						dibv.prepare();
						DocumentIterator stemIterator = new CachingDocumentIterator(new Term(tokenStem).accept(dibv));
						double logIdf = payloadTcr.logIdf(tokenStem, false);
						if (!Double.isInfinite(logIdf)) {
							docIterToTermWeight.put(stemIterator, logIdf);
							docIterToMatcherQuery.put(stemIterator, aq);
							if (mq.exist == Exist.may) {
								sumOrTermWeight.add(logIdf);
							}
						}
						else {
							logger.info(tokenStem + " not in index");
						}
						termIterators.add(stemIterator);
					}
					else {
						throw new IllegalArgumentException(aq + " has wrong type.");
					}
				}
				if (termIterators.size() > 0) {
					DocumentIterator phraseIterator = ConsecutiveDocumentIterator.getInstance(termIterators.toArray(new DocumentIterator[]{}));
					(mq.exist == Exist.may? orClauses : andClauses).add(phraseIterator);
					docIterToMatcherQuery.put(phraseIterator, pq);
				}
			}
			else {
				throw new IllegalArgumentException(mq + " has wrong type.");
			}
		} // for matcher
		final DocumentIterator preWandIterator;
		if (andClauses.isEmpty() && orClauses.isEmpty()) {
			logger.warn("empty query " + tq.queryId);
			preWandIterator = null;
		}
		else if (andClauses.isEmpty()) {
			preWandIterator = OrDocumentIterator.getInstance(orClauses.toArray(new DocumentIterator[]{}));
		}
		else if (orClauses.isEmpty()) {
			preWandIterator = AndDocumentIterator.getInstance(andClauses.toArray(new DocumentIterator[]{}));
		}
		else { 
			// both nonempty, let and be wand root, attach or node to it
			// note +a b is equivalent to +a +b
			andClauses.add(OrDocumentIterator.getInstance(orClauses.toArray(new DocumentIterator[]{})));
			preWandIterator = AndDocumentIterator.getInstance(andClauses.toArray(new DocumentIterator[]{})); 
		}
		/*
		 * If there is only one clause under the or node, it degenerates to and,
		 * in which case we zero out the total or weight.
		 * Note that at most one or node is possible in the query plan.
		 */
		if (orClauses.isEmpty() || orClauses.size() < 2) {
			sumOrTermWeight.setValue(0);
		}
		return preWandIterator;
	}
	
	private void executeOneQuery(RootQuery tq, DocumentIterator preWandIterator, MutableDouble sumOrTermWeight, Reference2ReferenceMap<DocumentIterator, MatcherQuery> docIterToMatcherQuery, Reference2DoubleOpenHashMap<DocumentIterator> docIterToTermWeight, File srDir) throws IOException, InstantiationException, IllegalAccessException {
		if (tq.contexts.size() != 1) throw new IllegalArgumentException("Must have exactly one " + ContextQuery.class.getCanonicalName() + " in query " + tq);
		final ContextQuery cq = tq.contexts.top();
		final double wandThreshold = wandThresholdFraction * sumOrTermWeight.doubleValue();
		SipSpanIterator ssi = sipIndex.getTypeIterator(yagoEntityTypeId);
		ReferenceArrayList<TypeBindingWitness> candWits = new ReferenceArrayList<TypeBindingWitness>(), surviveWits = new ReferenceArrayList<TypeBindingWitness>();
		Int2ReferenceRBTreeMap<TypeBindingWitness> witnessesSearchTree = new Int2ReferenceRBTreeMap<TypeBindingWitness>();
		ReferenceArrayList<DocumentIterator> iterators = new ReferenceArrayList<DocumentIterator>(docIterToMatcherQuery.keySet());
		logger.info("wand filter scan, " + docIterToTermWeight.size() + " term weights, sumOrDefined=" + sumOrTermWeight);
		boolean[] orPresent = new boolean[iterators.size()], allPresent = new boolean[iterators.size()];
		double presentTermWeight;
		long nDocs = 0, nWandDocs = 0, nWits = 0;
		Sip2Document s2d = new Sip2Document();
		for (; preWandIterator.hasNext(); ++nDocs) {
			final int doc = preWandIterator.nextDocument();
			if (doc == -1) { break; }
			Arrays.fill(orPresent, false);
			Arrays.fill(allPresent, false);
			collectPresent(preWandIterator, false, iterators, orPresent, allPresent);
			presentTermWeight = 0;
			for (int px = 0, pn = allPresent.length; px < pn; ++px) {
				if (orPresent[px]) {
					presentTermWeight += docIterToTermWeight.getDouble(iterators.get(px));
				}
			}
			if (presentTermWeight >= wandThreshold) {
				++nWandDocs;
				ssi.skipTo(doc);
				if (ssi.document() == doc) {
					/*
					 * http://www.thekevindolan.com/2010/02/interval-tree/index.html
					 * Might use an interval tree if it will cut down CPU cost.
					 * For now, given entity mention spans are often short and
					 * non-overlapping, we simply use a ordered search tree on
					 * the left endpoint of the entity mention span.
					 */
					s2d.setNull();
					ssi.getPostings(s2d);
					candWits.clear();
					/*
					 * There is no type binding query node in a telegraphic query,
					 * so we have to pass null.  This call gets only the witness
					 * for the entity mention, not supporting matchers.
					 */
					s2d.getWitnesses((TypeBindingQuery) null, doc, candWits);
					witnessesSearchTree.clear();
					for (TypeBindingWitness tbw : candWits) {
						witnessesSearchTree.put(tbw.interval.left, tbw);
					}
					surviveWits.clear();
					IntervalIterator wandIntervalIterator = preWandIterator.intervalIterator();
					for (Interval matchSpan = null; (matchSpan = wandIntervalIterator.nextInterval()) != null; ) {
						if (matchSpan.length() > cq.width) {
							continue; // this minimal span cannot be witness for any mention
						}
						final int supportLeft = Math.max(0, matchSpan.right - cq.width);
						final int supportRight = matchSpan.left + cq.width;
						// matchSpan is witness for mentions overlapping [supportLeft, supportRight]
						for (Int2ReferenceMap.Entry<TypeBindingWitness> tmx : witnessesSearchTree.tailMap(supportLeft).int2ReferenceEntrySet()) {
							TypeBindingWitness tbw = tmx.getValue();
							if (tbw.interval.right <= supportRight) {
								surviveWits.add(tbw);
							}
						}
					}
					nQueryIntervals.add(witnessesSearchTree.size());
					nQuerySurvivingIntervals.add(surviveWits.size());
					/*
					 * For each surviving TypeBindingWitness, prepare a ContextWitness.
					 * Note that we do not form a witness hierarchy. We need
					 * only the TokenLiteralWitnesses to form a feature vector.
					 * We also add the TypeBindingWitness to know the distances
					 * between entity mention and various literal matches.
					 */
					for (TypeBindingWitness tbw : surviveWits) {
						ReferenceArrayList<AWitness> cWitParts = new ReferenceArrayList<AWitness>();
						int tightLeft = tbw.interval.left, tightRight = tbw.interval.right;
						// this double loop is inefficient but easy to code as a first cut
						for (int px = 0, pn = allPresent.length; px < pn; ++px) {
							if (!allPresent[px]) continue;
							DocumentIterator di = iterators.get(px);
							MatcherQuery mq = docIterToMatcherQuery.get(di);
							if (!(mq instanceof TokenLiteralQuery)) continue;
							TokenLiteralQuery tlq = (TokenLiteralQuery) mq;
							IntervalIterator ii = di.intervalIterator();
							for (Interval possibleSupport = null; (possibleSupport = ii.nextInterval()) != null; ) {
								if (areNear(tbw.interval, possibleSupport, cq.width)) {
									tightLeft = Math.min(tightLeft, possibleSupport.left);
									tightRight = Math.max(tightRight, possibleSupport.right);
									TokenLiteralWitness tlw = new TokenLiteralWitness(doc, possibleSupport, tlq);
									cWitParts.add(tlw);
								}
							}
						}
						ContextWitness cw = new ContextWitness(doc, Interval.valueOf(tightLeft, tightRight), cq);
						cw.witnesses.add(tbw);
						cw.witnesses.addAll(cWitParts);
						++nWits;
						// that's it, the output is cw
						convertAndWrite(tq, cw, srDir);
					}
				}
			}
		}
		preWandIterator.dispose();
		logger.info("wand scan done docs=" + nWandDocs + "/" + nDocs + " cwits=" + nWits);
		nQueryDocs.add(nDocs);
		nQueryWandDocs.add(nWandDocs);
		ssi.dispose();
	}
	
	private boolean areNear(Interval ione, Interval itwo, int width) {
		int minLeft = Math.min(ione.left, itwo.left);
		int maxRight = Math.max(ione.right, itwo.right);
		return minLeft + width >= maxRight;
	}

	/**
	 * @param pdi
	 * @param orSubtree whether an explicit or node was seen as an ancestor of pdi
	 * @param iters input iterator array
	 * @param orPresent iterators present in (the one and only) or node
	 * @param allPresent present iterators in all nodes
	 */
	private void collectPresent(DocumentIterator pdi, boolean orSubtree, ReferenceArrayList<DocumentIterator> iters, boolean[] orPresent, boolean[] allPresent) {
		if (pdi instanceof AndDocumentIterator) {
			for (DocumentIterator kdi : ((AndDocumentIterator) pdi).documentIterator) {
				collectPresent(kdi, orSubtree, iters, orPresent, allPresent);
			}
		}
		else if (pdi instanceof ConsecutiveDocumentIterator) {
			for (DocumentIterator kdi : ((ConsecutiveDocumentIterator) pdi).documentIterator) {
				collectPresent(kdi, orSubtree, iters, orPresent, allPresent);
			}
		}
		else if (pdi instanceof OrDocumentIterator) {
			OrDocumentIterator odi = (OrDocumentIterator) pdi;
			int fSize = odi.computeFront();
			for (int fx = 0; fx < fSize; ++fx) {
				DocumentIterator kdi = odi.documentIterator[odi.front[fx]];
				collectPresent(kdi, true, iters, orPresent, allPresent);
			}			
		}
		else {
			final int pos = iters.indexOf(pdi);
			if (pos < 0 || pos >= iters.size()) {
				throw new IllegalArgumentException();
			}
			allPresent[pos] = true;
			if (orSubtree) { 
				orPresent[pos] = true;
			}
		}
	}

	/**
	 * We were running out of RAM buffering all {@link ContextWitness}es for one
	 * query, so we are going to dump them to sorted runs for
	 * {@link BitExternalMergeReduce}.
	 * @param tq 
	 * @param cw
	 * @param srDir
	 * @param bsrw
	 * @throws IOException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	private void convertAndWrite(RootQuery tq, ContextWitness cw, File srDir) throws IOException, InstantiationException, IllegalAccessException {
		final SketchySnippet.QEWC qewc = new SketchySnippet.QEWC();
		qewc.setNull();
		qewc.queryId.replace(tq.queryId);
		final SketchySnippet.QEN qen = new SketchySnippet.QEN();
		qen.setNull();
		qen.queryId.replace(tq.queryId);
		qen.numSnippets = 1;
		for (AWitness awit : cw.witnesses) {
			if (awit instanceof TypeBindingWitness) {
				final TypeBindingWitness tbw = (TypeBindingWitness) awit;
				qewc.ent = qen.ent = tbw.entLongId;
			}
			else if (awit instanceof TokenLiteralWitness) {
				final TokenLiteralWitness tlw = (TokenLiteralWitness) awit;
				final TokenLiteralQuery tlq = (TokenLiteralQuery) tlw.queryNode;
				// put, do not increment --- uma bugfix
				qewc.queryWordCount.put(tlq.tokenText, 1);
			}
			else {
				throw new IllegalArgumentException(awit.getClass().getCanonicalName());
			}
		}
		if (!bsrwQewc.append(qewc)) {
			saveAnotherRunFile(srDir, bsrwQewc, qewc.getClass());
			final boolean try2 = bsrwQewc.append(qewc);
			if (!try2) {
				throw new IOException();
			}
		}
		if (!bsrwQen.append(qen)) {
			saveAnotherRunFile(srDir, bsrwQen, qen.getClass());
			final boolean try2 = bsrwQen.append(qen);
			if (!try2) {
				throw new IOException();
			}
		}
	}
	
	/**
	 * To reduce coding hassles, use {@link BitSortedRunWriter} to dump a temporary 
	 * file, then aggregate using {@link BitExternalMergeReduce} and throw away
	 * the temporary file. Would be better to aggregate on the fly before writing
	 * a file.
	 */
	private <A extends IBitReducible<A>> void saveAnotherRunFile(File srDir, BitSortedRunWriter<A> bsrw, Class<? extends A> recType) throws IOException, InstantiationException, IllegalAccessException {
		File tmpRunFile = File.createTempFile(recType.getCanonicalName() + "_temp_", ".dat", srDir);
		OutputBitStream obs = new OutputBitStream(tmpRunFile);  
		bsrw.flushSortedRun(obs);
		obs.close();
		File qewcRunFile = new File(srDir, recType.getCanonicalName() + "_" + stripeManager.myDiskStripe() + "_" + runGen.incrementAndGet() + ".dat"); 
		BitExternalMergeReduce<A, A> bemr = new BitExternalMergeReduce<A, A>(recType, recType, srDir);
		bemr.reduceUsingHeap(Arrays.asList(tmpRunFile), recType.newInstance().getReducer(), qewcRunFile);
		if (tmpRunFile.length() == 0 || qewcRunFile.length() > 0) {
			tmpRunFile.delete();
		}
	}
}
