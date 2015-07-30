package iitb.CSAW.Search;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.PhraseWitness;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Index.SIP2.Sip2IndexReader;
import iitb.CSAW.Query.AtomQuery;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.IQuery;
import iitb.CSAW.Query.IQueryVisitor;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.Reference2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceOpenHashSet;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.util.Interval;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class BaseQueryProcessor {
	static final long MILLION = 1000000;
	protected final Logger logger = Logger.getLogger(getClass());
	protected final Config config;
	final AStripeManager stripeManager;
	public final ACatalog catalog;
	protected final TermProcessor termProcessor;
	final IntOpenHashSet catIdSubset; 

	final Index stemIndex;
	/** Over the payload corpus, not the reference corpus. */
	final TokenCountsReader payloadTcr;
	public final Sip2IndexReader sipIndexReader;
	
	final HashMap<MutableString, ReferenceArrayList<TokenLiteralQuery>> stemToMatcher = new HashMap<MutableString, ReferenceArrayList<TokenLiteralQuery>>();
	final HashMap<String, ReferenceArrayList<EntityLiteralQuery>> entNameToMatcher = new HashMap<String, ReferenceArrayList<EntityLiteralQuery>>();
	final HashMap<String, ReferenceArrayList<TypeBindingQuery>> typeNameToMatcher = new HashMap<String, ReferenceArrayList<TypeBindingQuery>>();
	
	public BaseQueryProcessor(Config config) throws RuntimeException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, ConfigurationException, IOException, URISyntaxException, InstantiationException, EnvironmentLockedException, DatabaseException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		this.catalog = ACatalog.construct(config);
		this.termProcessor = DefaultTermProcessor.construct(config);
		
		if (config.containsKey("TypeSubsetFile")) {
			catIdSubset = new IntOpenHashSet();
			for (MutableString catName : new FileLinesCollection(config.getString("TypeSubsetFile"), "UTF-8")) {
				final int catId = catalog.catNameToCatID(catName.toString());
				if (catId >= 0) {
					catIdSubset.add(catId);
				}
			}
			logger.warn("Subsetting index to " + catIdSubset.size() + " types");
		}
		else {
			catIdSubset = null;
		}

		final URI tokenIndexUri = stripeManager.tokenIndexDiskDir(stripeManager.myDiskStripe());
		assert tokenIndexUri.getHost().equals(stripeManager.myHostName());
		final File tokenIndexDiskDir = new File(tokenIndexUri.getPath());
		final String stemName = Field.token.toString();
		final String stemBase = (new File(tokenIndexDiskDir, stemName)).toString();
		stemIndex = Index.getInstance(stemBase, true);
		this.payloadTcr = new TokenCountsReader(tokenIndexUri);
		this.sipIndexReader = new Sip2IndexReader(config);
	}
	
	protected void decorate(RootQuery rootQuery) {
		for (ContextQuery context : rootQuery.contexts) {
			for (MatcherQuery matcher : context.matchers) {
				decorate(matcher);
			}
		}
	}
	
	void decorate(MatcherQuery matcher) {
		if (matcher instanceof EntityLiteralQuery) {
			final EntityLiteralQuery cel = (EntityLiteralQuery) matcher;
			final int entId = catalog.entNameToEntID(cel.entName);
			if (entId < 0) {
				throw new IllegalArgumentException("Bad entity " + cel.entName + " in " + matcher);
			}
			final long corpusFreq = sipIndexReader.entGlobalCf(entId, false);
			final long nSlots = sipIndexReader.entGlobalCf();
			final long documentFreq = sipIndexReader.entGlobalDf(entId, false);
			final long nDocs = sipIndexReader.entGlobalDf();
			cel.decorate(entId, corpusFreq, nSlots, documentFreq, nDocs);
		}
		else if (matcher instanceof TokenLiteralQuery) {
			final TokenLiteralQuery ctl = (TokenLiteralQuery) matcher;
			final MutableString tokenStem = new MutableString(ctl.tokenText);
			if (!termProcessor.processTerm(tokenStem)) {
				throw new IllegalArgumentException(ctl + " cannot be decorated");
			}
			final long corpusFreq = payloadTcr.globalCorpusFrequency(tokenStem, false);
			final long nSlots = payloadTcr.globalCorpusFrequency();
			final long documentFreq = payloadTcr.globalDocumentFrequency(tokenStem, false);
			final long nDocs = payloadTcr.globalDocumentFrequency();
			ctl.decorate(tokenStem, corpusFreq, nSlots, documentFreq, nDocs);
		}
		else if (matcher instanceof TypeBindingQuery) {
			final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcher;
			final int typeId = catalog.catNameToCatID(typeBindingQuery.typeName);
			if (typeId < 0) {
				throw new IllegalArgumentException("Bad type in " + matcher);				
			}
			final long corpusFreq = sipIndexReader.typeGlobalCf(typeId, false);
			final long nSlots = sipIndexReader.typeGlobalCf();
			final long documentFreq = sipIndexReader.typeGlobalDf(typeId, false);
			final long nDocs = sipIndexReader.typeGlobalDf();
			typeBindingQuery.decorate(typeId, corpusFreq, nSlots, documentFreq, nDocs);
		}
		else if (matcher instanceof PhraseQuery) {
			final PhraseQuery cp = (PhraseQuery) matcher;
			for (AtomQuery atom : cp.atoms) {
				decorate(atom);
			}
		}
	}

	
	public void purgeQueryState(RootQuery rootQuery) {
		rootQuery.removeDocumentState();
		stemToMatcher.clear();
		entNameToMatcher.clear();
		typeNameToMatcher.clear();
	}
	
	/**
	 * Initially we will support restricted families of queries.
	 * @param csawQuery
	 */
	void checkValidSingleEntityQuery(final RootQuery csawQuery) {
		// exactly one type binding
		final MutableInt nTypeBindings = new MutableInt(0);
		csawQuery.visit(new IQueryVisitor() {
			@Override
			public void visit(MatcherQuery matcher) {
				if (matcher instanceof TypeBindingQuery) {
					final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcher;
					final int catId = catalog.catNameToCatID(typeBindingQuery.typeName);
					if (catId < 0) {
						throw new IllegalArgumentException("Catalog cannot map " + typeBindingQuery.typeName);
					}
					if (catIdSubset != null && !catIdSubset.contains(catId)) {
						throw new IllegalArgumentException("Type " + typeBindingQuery.typeName + " ID=" + catId + " not in indexed subset");
					}
					nTypeBindings.increment();
				}
				if (matcher.exist == Exist.not) {
					throw new IllegalArgumentException("Cannot have not clauses in " + csawQuery);
				}
				if (matcher instanceof PhraseQuery) {
					final PhraseQuery phrase = (PhraseQuery) matcher;
					for (AtomQuery atom : phrase.atoms) {
						if (atom instanceof TypeBindingQuery) {
							throw new IllegalArgumentException("Cannot have type binding within phrase in " + csawQuery);
						}
					}
				}
			}
		});
		if (nTypeBindings.intValue() != 1) {
			throw new IllegalArgumentException("Invalid single entity query " + csawQuery);
		}
		// same literal cannot appear multiple times?
	}

	/**
	 * This method does not depend on restricted query classes.
	 * @param csawQuery
	 */
	void constructLeafMaps(RootQuery csawQuery) {
		stemToMatcher.clear();
		entNameToMatcher.clear();
		typeNameToMatcher.clear();
		for (ContextQuery context : csawQuery.contexts) {
			for (MatcherQuery matcher : context.matchers) {
				constructLeafMaps(matcher);
			}
		}
	}

	/**
	 * This method does not depend on restricted query classes.
	 * Leaf nodes to be matched first are {@link TokenLiteralQuery},
	 * {@link EntityLiteralQuery}, {@link TypeBindingQuery}.
	 * A {@link TokenLiteralQuery} will match a single token but the
	 * rest can span multiple tokens so even the witness
	 * attached at some leaf nodes may have length more than 1.
	 * It's rare to repeat a literal in a query, but possible. In that case,
	 * a single token to match may feed into multiple leaf nodes of the AST. 
	 * 
	 * @param matcher
	 */
	void constructLeafMaps(MatcherQuery matcher) {
		if (matcher instanceof EntityLiteralQuery) {
			EntityLiteralQuery cel = (EntityLiteralQuery) matcher;
			augment(entNameToMatcher, cel.entName, cel);
		}
		else if (matcher instanceof TokenLiteralQuery) {
			TokenLiteralQuery ctl = (TokenLiteralQuery) matcher;
			augment(stemToMatcher, ctl.tokenStem, ctl);
		}
		else if (matcher instanceof TypeBindingQuery) {
			TypeBindingQuery ctb = (TypeBindingQuery) matcher;
			augment(typeNameToMatcher, ctb.typeName, ctb);
		}
		else if (matcher instanceof PhraseQuery) {
			for (AtomQuery atom : ((PhraseQuery) matcher).atoms) {
				constructLeafMaps(atom);
			}
		}
	}

	<K, V> void augment(HashMap<K, ReferenceArrayList<V>> map, K key, V val) {
		ReferenceArrayList<V> vals;
		if (map.containsKey(key)) {
			vals = map.get(key);
		}
		else {
			vals = new ReferenceArrayList<V>();
			map.put(key, vals);
		}
		vals.add(val);
	}

	/**
	 * Sort leaf witnesses in increasing order of left endpoint of interval.
	 */
	void sortLeafWitnesses(RootQuery rootQuery) {
		for (ContextQuery contextQuery : rootQuery.contexts) {
			for (MatcherQuery matcherQuery : contextQuery.matchers) {
				if (matcherQuery instanceof PhraseQuery) {
					final PhraseQuery phraseQuery = (PhraseQuery) matcherQuery;
					for (AtomQuery atomQuery : phraseQuery.atoms) {
						Collections.sort(atomQuery.witnesses);
					}
				}
				else {
					Collections.sort(matcherQuery.witnesses);
				}
			}
		}
	}
	
	void percolate(int docId, RootQuery query) {
		for (ContextQuery context : query.contexts) {
			for (MatcherQuery matcher : context.matchers) {
				if (matcher instanceof PhraseQuery) {
					final long t0 = System.nanoTime();
//					percolateOnePhrase((PhraseQuery) matcher);
					ReferenceArrayList<AWitness> phraseWitnesses = new ReferenceArrayList<AWitness>();
					percolateOnePhraseExtend(docId, (PhraseQuery) matcher, 0, -1, phraseWitnesses);
					final long t1 = System.nanoTime();
					MonitorFactory.add("percolateOnePhrase", "ms", (t1 - t0)/MILLION);
				}
			}
			final long t2 = System.nanoTime();
			percolateOneContext(docId, context);
			final long t3 = System.nanoTime();
			MonitorFactory.add("percolateOneContext", "ms", (t3 - t2)/MILLION);
		}
	}
	
	void percolateOnePhraseExtend(int docId, PhraseQuery phraseQuery, int atomPos, int lookForLeft, ReferenceArrayList<AWitness> phraseWitnesses) {
		if (atomPos == phraseQuery.atoms.size()) {
			// completed one phrase match
			logger.debug("phrase match! " + phraseWitnesses);
			final int lpx = phraseWitnesses.get(0).interval.left, rpx = phraseWitnesses.top().interval.right;
			final PhraseWitness pw = new PhraseWitness(docId, Interval.valueOf(lpx, rpx), phraseQuery);
			pw.atomWitnesses.clear();
			pw.atomWitnesses.addAll(phraseWitnesses);
			phraseQuery.witnesses.add(pw);
			return;
		}
		final AtomQuery atomQuery = phraseQuery.atoms.get(atomPos);
		for (AWitness nextWitness : atomQuery.witnesses) {
			if (atomPos > 0 && nextWitness.interval.left > lookForLeft) break;
			if (atomPos == 0 || nextWitness.interval.left == lookForLeft) {
				phraseWitnesses.push(nextWitness);
				percolateOnePhraseExtend(docId, phraseQuery, atomPos + 1, nextWitness.interval.right + 1, phraseWitnesses);
				phraseWitnesses.pop();
			}
		}
	}
	
	/**
	 * <b>Note</b> that this method works only for queries with a <b>single</b> binding outside any phrases.
	 * @param context
	 */
	void percolateOneContext(int docId, ContextQuery context) {
		if (context.window != ContextQuery.Window.unordered) {
			throw new IllegalArgumentException("Only " + ContextQuery.Window.unordered + " windows supported");
		}
		final ReferenceArrayList<AWitness> mWs = new ReferenceArrayList<AWitness>();
		for (MatcherQuery matcher : context.matchers) {
			mWs.addAll(matcher.witnesses);
		}
		Collections.sort(mWs);
		logger.debug("matcherWitnesses = " + mWs);
		for (int bx = 0; bx < mWs.size(); ++bx) {
			final AWitness bw = mWs.get(bx);
			if (bw instanceof TypeBindingWitness) {
				percolateOneContextAtBinding(docId, context, mWs, bx);
			}
		}
	}

	/**
	 * <b>Note</b> that this method works only for queries with a <b>single</b> binding outside any phrases.
	 */
	private void percolateOneContextAtBinding(int docId, ContextQuery context, List<AWitness> mWs, final int bx) {
		final TypeBindingWitness tbw = (TypeBindingWitness) mWs.get(bx);
		final TypeBindingQuery tbq = (TypeBindingQuery) tbw.queryNode;
		Reference2BooleanOpenHashMap<IQuery> mustMatcherFound = new Reference2BooleanOpenHashMap<IQuery>(context.matchers.size());
		ReferenceOpenHashSet<AWitness> mustWitnesses = new ReferenceOpenHashSet<AWitness>(context.matchers.size());
		ReferenceOpenHashSet<IQuery> mayMatchers = new ReferenceOpenHashSet<IQuery>(context.matchers.size());
		ReferenceOpenHashSet<AWitness> mayWitnesses = new ReferenceOpenHashSet<AWitness>(context.matchers.size());
		int nMayMatcherFound = 0;
		for (MatcherQuery matcher : context.matchers) {
			if (matcher == tbq) continue;
			if (matcher.exist == Exist.must) {
				mustMatcherFound.put(matcher, false);
			}
			else if (matcher.exist == Exist.may) {
				mayMatchers.add(matcher);
			}
		}
		int lcon = tbw.interval.left, rcon = tbw.interval.right;
		// look to the left of bx
		for (int sx = bx - 1; sx >= 0; --sx) {
			final AWitness awit = mWs.get(sx);
			if (awit.interval.left < tbw.interval.right - context.width) {
				break;
			}
			if (mustMatcherFound.containsKey(awit.queryNode)) {
				mustMatcherFound.put(awit.queryNode, true);
				mustWitnesses.add(awit); // could add redundant must witnesses
				lcon = Math.min(lcon, awit.interval.left);
			}
			if (mayMatchers.contains(awit.queryNode)) {
				++nMayMatcherFound;
				mayWitnesses.add(awit);
				lcon = Math.min(lcon, awit.interval.left);
			}
		}
		// look to the right of bx
		for (int sx = bx + 1; sx < mWs.size(); ++sx) {
			final AWitness awit = mWs.get(sx);
			if (awit.interval.right > tbw.interval.left + context.width) {
				break;
			}
			if (mustMatcherFound.containsKey(awit.queryNode)) {
				mustMatcherFound.put(awit.queryNode, true);
				mustWitnesses.add(awit); // could add redundant must witnesses
				rcon = Math.max(rcon, awit.interval.right);
			}
			if (mayMatchers.contains(awit.queryNode)) {
				++nMayMatcherFound;
				mayWitnesses.add(awit);
				rcon = Math.max(rcon, awit.interval.right);
			}
		}
		for (boolean found : mustMatcherFound.values()) {
			if (!found) return;
		}
		if (mustMatcherFound.isEmpty() && nMayMatcherFound == 0) {
			return; // we might choose to change this policy
		}
		final ContextWitness cwit = new ContextWitness(docId, Interval.valueOf(lcon, rcon), context);
		cwit.witnesses.add(tbw);
		cwit.witnesses.addAll(mustWitnesses);
		cwit.witnesses.addAll(mayWitnesses);
		context.witnesses.add(cwit);
	}
	
	void collectWitnesses(RootQuery rootQuery, List<ContextWitness> cwits) {
		for (ContextQuery contextQuery : rootQuery.contexts) {
			for (AWitness witness : contextQuery.witnesses) {
				cwits.add((ContextWitness) witness);
			}
		}
	}
	
	void clearWitnessesFromQueryPlan(RootQuery rootQuery) {
		for (ContextQuery contextQuery : rootQuery.contexts) {
			for (MatcherQuery matcherQuery : contextQuery.matchers) {
				clearWitnessesFromQueryPlan(matcherQuery);
			}
			contextQuery.witnesses.clear();
		}
	}

	private void clearWitnessesFromQueryPlan(MatcherQuery matcherQuery) {
		if (matcherQuery instanceof PhraseQuery) {
			final PhraseQuery phraseQuery = (PhraseQuery) matcherQuery;
			for (AtomQuery atomQuery : phraseQuery.atoms) {
				clearWitnessesFromQueryPlan(atomQuery);
			}
		}
		matcherQuery.witnesses.clear();
	}
}
