package iitb.CSAW.Search;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntIterator;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.TokenLiteralWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Index.SIP2.Sip2Document;
import iitb.CSAW.Index.SIP2.Sip2IndexReader.SipSpanIterator;
import iitb.CSAW.Query.AtomQuery;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.mg4j.index.IndexIterator;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Level;

import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

/**
 * Implements (initially single type binding) query processor over token and
 * entity literals using MG4J postings and type binding using our SIP index.
 * <strong>Caution:</strong> Not thread-safe. In particular, modifies decorations
 * on the input query AST, executes, then clears away decorations. So the same
 * query cannot even be passed into two instances of {@link ProbeQueryProcessor}. 
 * @author soumen
 * @since 2010/09/13
 */
public class ProbeQueryProcessor extends BaseQueryProcessor {
	static final boolean bePedantic = false;
	
	public ProbeQueryProcessor(Config config) throws RuntimeException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, ConfigurationException, IOException, URISyntaxException, InstantiationException, EnvironmentLockedException, DatabaseException {
		super(config);
	}
	
	public void execute(RootQuery rootQuery, List<ContextWitness> cwits) throws IOException {
		decorate(rootQuery);
		cwits.clear();
		checkValidSingleEntityQuery(rootQuery);
		constructLeafMaps(rootQuery);
		ReferenceArrayList<MatcherQuery> leafMustMatchers = new ReferenceArrayList<MatcherQuery>();
		ReferenceArrayList<MatcherQuery> leafMayMatchers = new ReferenceArrayList<MatcherQuery>();		
		for (ContextQuery contextQuery : rootQuery.contexts) {
			for (MatcherQuery matcherQuery : contextQuery.matchers) {
				switch (matcherQuery.exist) {
				case may:
					populateLeafIterators(matcherQuery, leafMayMatchers);
					break;
				case must:
					populateLeafIterators(matcherQuery, leafMustMatchers);
					break;
				default:
					logger.fatal("cannot support " + matcherQuery.exist + " " + matcherQuery);
				}
			}
		}

		TIntHashSet mergeDocSet = new TIntHashSet();
		for (MutableInt docId = new MutableInt(0); docId.intValue() < Integer.MAX_VALUE; ) {
			final Level remLevel = logger.getLevel();
			if (Arrays.asList().contains(rootQuery.queryId)) {
				logger.setLevel(Level.TRACE);
			}
			alignMustIterators(docId, leafMustMatchers);
			logger.setLevel(remLevel);
			if (docId.intValue() >= Integer.MAX_VALUE) {
				break;
			}
			mergeDocSet.add(docId.intValue());
			populateMustLeafWitnesses(docId.intValue(), leafMustMatchers);
			alignMayIteratorsAndPopulateMayLeafWitnesses(docId.intValue(), leafMayMatchers);
			percolate(docId.intValue(), rootQuery);
			collectWitnesses(rootQuery, cwits);
			clearWitnessesFromQueryPlan(rootQuery);
			docId.increment();
		}
		logger.debug(mergeDocSet.size() + " docs satisfy must matchers as per merge");
		MonitorFactory.add("wholeDocMustMatch", null, mergeDocSet.size());
		
		if (bePedantic) {
			TIntIntHashMap bfmi = new TIntIntHashMap();
			bruteForceMustIntersect(leafMustMatchers, bfmi);
			logger.debug("brute force = " + bfmi.size() + ", merge = " + mergeDocSet.size());
			for (int testDoc : bfmi.keys()) {
				if (!mergeDocSet.contains(testDoc)) {
					logger.fatal(testDoc + " in brute not merge");
				}
			}
			for (int testDoc : mergeDocSet.toArray()) {
				if (!bfmi.containsKey(testDoc)) {
					logger.fatal(testDoc + " in merge not in brute");
				}
			}
		}
		
		for (ContextQuery contextQuery : rootQuery.contexts) {
			for (MatcherQuery matcherQuery : contextQuery.matchers) {
				disposeLeafIterators(matcherQuery);
			}
		}
	}

	/**
	 * @param docId
	 * @param matcherQueries iterators should be all aligned with docId already
	 * @throws IOException 
	 */
	void populateMustLeafWitnesses(int docId, List<MatcherQuery> matcherQueries) throws IOException {
		for (MatcherQuery matcherQuery : matcherQueries) {
			if (matcherQuery instanceof TokenLiteralQuery) {
				final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
				assert docId == tokenLiteralQuery.indexIterator.document();
				tokenLiteralQuery.witnesses.clear();
				for (IntIterator posx = tokenLiteralQuery.indexIterator.positions(); posx.hasNext(); ) {
					final int pos = posx.nextInt();
					final TokenLiteralWitness tokenLiteralWitness = new TokenLiteralWitness(docId, Interval.valueOf(pos), tokenLiteralQuery);
					tokenLiteralQuery.witnesses.add(tokenLiteralWitness);
				}
				MonitorFactory.add(TokenLiteralWitness.class.getSimpleName(), null, tokenLiteralQuery.witnesses.size());				
			}
			else if (matcherQuery instanceof EntityLiteralQuery) {
				final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
				assert docId == entityLiteralQuery.sipIterator.document();
				final Sip2Document ssd = new Sip2Document();
				entityLiteralQuery.sipIterator.getPostings(ssd);
				assert docId == ssd.docId();
				entityLiteralQuery.witnesses.clear();
				ssd.getWitnesses(entityLiteralQuery, docId, entityLiteralQuery.witnesses);
				MonitorFactory.add(EntityLiteralWitness.class.getSimpleName(), null, entityLiteralQuery.witnesses.size());
			}
			else if (matcherQuery instanceof TypeBindingQuery) {
				final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
				// type bindings may also span multiple adjacent tokens but need to also check entID
				final Sip2Document idp = new Sip2Document();
				typeBindingQuery.sipIterator.getPostings(idp);
				assert docId == idp.docId();
				typeBindingQuery.witnesses.clear();
				idp.getWitnesses(typeBindingQuery, docId, typeBindingQuery.witnesses);
				MonitorFactory.add(TypeBindingWitness.class.getSimpleName(), null, typeBindingQuery.witnesses.size());				
			}
		}
	}
	
	/* Method convertEntityPostingsToIntervals is no longer required. */

	/**
	 * @param docId In/out. If there is a document with ID at least docId where
	 * all mustLeafMatchers match, the smallest such ID is returned. If there
	 * isn't, docId is set to {@link Integer#MAX_VALUE}. 
	 * @param mustLeafMatchers
	 * @throws IOException
	 */
	void alignMustIterators(MutableInt docId, Collection<MatcherQuery> mustLeafMatchers) throws IOException {
		logger.debug("begin align " + docId);
		while (docId.intValue() < Integer.MAX_VALUE) {
			final int prevDocId = docId.intValue();
			for (MatcherQuery matcherQuery : mustLeafMatchers) {
				if (matcherQuery instanceof TokenLiteralQuery) {
					final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
					final int retDocId = tokenLiteralQuery.indexIterator.skipTo(docId.intValue());
					logger.trace(tokenLiteralQuery + " skipTo " + docId + " " + retDocId);
					if (retDocId < docId.intValue()) {
						logger.warn(tokenLiteralQuery + " skipTo(" + docId + ") = " + retDocId);
						// we will assume this means end of tether
						docId.setValue(Integer.MAX_VALUE);
						return;
					}
					assert retDocId >= docId.intValue();
					docId.setValue(retDocId);
					if (retDocId == Integer.MAX_VALUE) {
						return;
					}
					logger.trace("\t" + docId + " " + tokenLiteralQuery);
				}
				else if (matcherQuery instanceof EntityLiteralQuery) {
					final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
					while (entityLiteralQuery.sipIterator.document() < docId.intValue()) {
						if (entityLiteralQuery.sipIterator.nextDocument() == Integer.MAX_VALUE) {
							docId.setValue(Integer.MAX_VALUE);
							return;
						}
					}
					docId.setValue(entityLiteralQuery.sipIterator.document());
					logger.trace("\t" + docId + " " + entityLiteralQuery);
				}
				else if (matcherQuery instanceof TypeBindingQuery) {
					final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
					while (typeBindingQuery.sipIterator.document() < docId.intValue()) {
						if (typeBindingQuery.sipIterator.nextDocument() == Integer.MAX_VALUE) {
							docId.setValue(Integer.MAX_VALUE);
							return;
						}
					}
					docId.setValue(typeBindingQuery.sipIterator.document());
					logger.trace("\t" + docId + " " + typeBindingQuery);
				}
			} // for each matcher
			assert docId.intValue() >= prevDocId;
			// after one pass through all must iterators
			if (docId.intValue() == prevDocId) {
				logger.debug("end align " + docId);
				if (bePedantic) {
					doubleCheckMustIterators(docId.intValue(), mustLeafMatchers);
				}
				return;
			}
		} // until stability
		logger.fatal("blimey");
	}
	
	private void doubleCheckMustIterators(int docId, Collection<MatcherQuery> mustLeafMatchers) {
		for (MatcherQuery mustMatcherQuery : mustLeafMatchers) {
			if (mustMatcherQuery instanceof TokenLiteralQuery) {
				final TokenLiteralQuery tlq = (TokenLiteralQuery) mustMatcherQuery;
				assert docId == tlq.indexIterator.document();
			}
			else if (mustMatcherQuery instanceof EntityLiteralQuery) {
				final EntityLiteralQuery elq = (EntityLiteralQuery) mustMatcherQuery;
				assert docId == elq.sipIterator.document();
			}
			else if (mustMatcherQuery instanceof TypeBindingQuery) {
				final TypeBindingQuery tbq = (TypeBindingQuery) mustMatcherQuery;
				assert docId == tbq.sipIterator.document();
			}
		}
	}

	void bruteForceMustIntersect(final List<MatcherQuery> mustMatchers, TIntIntHashMap another) throws IOException {
		another.clear();
		TypeBindingQuery anchor = null;
		for (MatcherQuery matcherQuery : mustMatchers) {
			if (matcherQuery instanceof TypeBindingQuery) {
				anchor = (TypeBindingQuery) matcherQuery;
				break;
			}
		}
		/* anchor */ {
			final SipSpanIterator sii = sipIndexReader.getTypeIterator(anchor.typeId);
			for (int docId = -1; (docId = sii.nextDocument()) != Integer.MAX_VALUE; ) {
				another.adjustOrPutValue(docId, 1, 1);
			}
			sii.dispose();
		}
		logger.info("started with " + another.size() + " docs for " + anchor);
		// now subtract others
		for (MatcherQuery matcherQuery : mustMatchers) {
			if (matcherQuery == anchor) continue;
			if (matcherQuery instanceof TokenLiteralQuery) {
				final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
				int nDocs = 0;
				final IndexIterator indexIterator = stemIndex.documents(tokenLiteralQuery.tokenStem);
				while (indexIterator.hasNext()) {
					final int docId = indexIterator.nextDocument();
					another.adjustOrPutValue(docId, 1, 1);
					++nDocs;
				}
				indexIterator.dispose();
				logger.info("scanned " + nDocs + " docs for " + matcherQuery);
			}
			else if (matcherQuery instanceof EntityLiteralQuery) {
				final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
				final SipSpanIterator sii = sipIndexReader.getEntIterator(entityLiteralQuery.entId);
				int nDocs = 0;
				for (int docId = -1; (docId = sii.nextDocument()) != Integer.MAX_VALUE; ) {
					another.adjustOrPutValue(docId, 1, 1);
					++nDocs;
				}
				sii.dispose();
				logger.info("scanned " + nDocs + " docs for " + matcherQuery);
			}
			else if (matcherQuery instanceof TypeBindingQuery) {
				final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
				final SipSpanIterator sii = sipIndexReader.getTypeIterator(typeBindingQuery.typeId);
				int nDocs = 0;
				for (int docId = -1; (docId = sii.nextDocument()) != Integer.MAX_VALUE; ) {
					another.adjustOrPutValue(docId, 1, 1);
					++nDocs;
				}
				sii.dispose();
				logger.info("scanned " + nDocs + " docs for " + matcherQuery);
			}
		}
		final int preSize = another.size();
		for (TIntIntIterator ax = another.iterator(); ax.hasNext(); ) {
			ax.advance();
			if (ax.value() < mustMatchers.size()) {
				ax.remove();
			}
		}
		logger.info("cut down union=" + preSize + " to intersection=" + another.size());
	}
	
	private void populateLeafIterators(MatcherQuery matcherQuery, ReferenceArrayList<MatcherQuery> leafMatchers) throws IOException {
		if (matcherQuery instanceof PhraseQuery) {
			final PhraseQuery phraseQuery = (PhraseQuery) matcherQuery;
			for (AtomQuery atomQuery : phraseQuery.atoms) {
				populateLeafIterators(atomQuery, leafMatchers);
			}
		}
		else if (matcherQuery instanceof TokenLiteralQuery) {
			final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
			tokenLiteralQuery.indexIterator = stemIndex.documents(tokenLiteralQuery.tokenStem);
			leafMatchers.add(matcherQuery);
		}
		else if (matcherQuery instanceof EntityLiteralQuery) {
			final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
			entityLiteralQuery.sipIterator = sipIndexReader.getEntIterator(entityLiteralQuery.entId);
			leafMatchers.add(matcherQuery);
		}
		else if (matcherQuery instanceof TypeBindingQuery) {
			final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
			typeBindingQuery.sipIterator = sipIndexReader.getTypeIterator(typeBindingQuery.typeId);
			leafMatchers.add(matcherQuery);
		}
	}
	
	private void disposeLeafIterators(MatcherQuery matcherQuery) throws IOException {
		if (matcherQuery instanceof PhraseQuery) {
			final PhraseQuery phraseQuery = (PhraseQuery) matcherQuery;
			for (AtomQuery atomQuery : phraseQuery.atoms) {
				disposeLeafIterators(atomQuery);
			}
		}
		else if (matcherQuery instanceof TokenLiteralQuery) {
			final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
			tokenLiteralQuery.indexIterator.dispose();
		}
		else if (matcherQuery instanceof EntityLiteralQuery) {
			final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
			entityLiteralQuery.sipIterator.dispose();
		}
		else if (matcherQuery instanceof TypeBindingQuery) {
			final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
			typeBindingQuery.sipIterator.dispose();
		}
	}
	
	/**
	 * Advance each matcher in mayLeafMatchers to the given docId. If overshot, stop
	 * at the earliest detected overshoot. No iterative update to docId is needed.
	 * @param docId in only, not out
	 * @param mayLeafMatchers
	 * @throws IOException
	 */
	void alignMayIteratorsAndPopulateMayLeafWitnesses(int docId, Collection<MatcherQuery> mayLeafMatchers) throws IOException {
		int nMayFound = 0;
		for (MatcherQuery matcherQuery : mayLeafMatchers) {
			if (matcherQuery instanceof TokenLiteralQuery) {
				final TokenLiteralQuery tokenLiteralQuery = (TokenLiteralQuery) matcherQuery;
				for (;;) {
					final int curDoc = tokenLiteralQuery.indexIterator.document();
					if (curDoc < docId) {
						final int nextDoc = tokenLiteralQuery.indexIterator.nextDocument();
						if (nextDoc == -1) {
							break;
						}
					}
					else if (curDoc == docId) {
						++nMayFound;
						tokenLiteralQuery.witnesses.clear();
						for (IntIterator posx = tokenLiteralQuery.indexIterator.positions(); posx.hasNext(); ) {
							final int pos = posx.nextInt();
							final TokenLiteralWitness tokenLiteralWitness = new TokenLiteralWitness(docId, Interval.valueOf(pos), tokenLiteralQuery);
							tokenLiteralQuery.witnesses.add(tokenLiteralWitness);
						}
						MonitorFactory.add(TokenLiteralWitness.class.getSimpleName(), null, tokenLiteralQuery.witnesses.size());
						break;
					}
					else { // curDoc > docId
						break;
					}
				}
			}
			else if (matcherQuery instanceof EntityLiteralQuery) {
				final EntityLiteralQuery entityLiteralQuery = (EntityLiteralQuery) matcherQuery;
				for (;;) {
					final int curDoc = entityLiteralQuery.sipIterator.document();
					if (curDoc < docId) {
						final int nextDoc = entityLiteralQuery.sipIterator.nextDocument();
						if (nextDoc == Integer.MAX_VALUE) {
							break;
						}
					}
					else if (curDoc == docId) {
						++nMayFound;
						final Sip2Document ssd = new Sip2Document();
						entityLiteralQuery.sipIterator.getPostings(ssd);
						assert ssd.docId() == docId;
						ssd.getWitnesses(entityLiteralQuery, docId, entityLiteralQuery.witnesses);
						MonitorFactory.add(EntityLiteralWitness.class.getSimpleName(), null, entityLiteralQuery.witnesses.size());
					}
					else { // curDoc > docId
						break;
					}
				}
			}
			else {
				throw new IllegalArgumentException(matcherQuery + " cannot be a may leaf.");
			}
		} // for matcher
		logger.debug(nMayFound + " of " + mayLeafMatchers.size() + " may matchers found");
	}
}
