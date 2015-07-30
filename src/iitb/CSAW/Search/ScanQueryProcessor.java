package iitb.CSAW.Search;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.TokenLiteralWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.mutable.MutableInt;

import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

/**
 * Given a (decorated) {@link RootQuery} and access to all required
 * field views of an {@link IDocument}, constructs {@link AWitness}es
 * with links to the {@link RootQuery}, if a qualifying interval exists.
 * Crude initial implementation, instantiates arrays of witnesses at each AST
 * node instead of pulling on cursors.
 * @author soumen
 */

public class ScanQueryProcessor extends BaseQueryProcessor {
	public ScanQueryProcessor(Config config) throws RuntimeException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, ConfigurationException, EnvironmentLockedException, IOException, URISyntaxException, InstantiationException, DatabaseException {
		super(config);
	}
	
	public void execute(RootQuery rootQuery, IAnnotatedDocument doc, TermProcessor stemTermProcessor, ReferenceArrayList<ContextWitness> cwits) {
		decorate(rootQuery);
		cwits.clear();
		checkValidSingleEntityQuery(rootQuery);
		final long t0 = System.nanoTime();
		constructLeafMaps(rootQuery);
		final long t1 = System.nanoTime();
		populateLeafWitnesses(doc, stemTermProcessor);
		sortLeafWitnesses(rootQuery);
		final long t2 = System.nanoTime();
		percolate(doc.docidAsInt(), rootQuery);
		collectWitnesses(rootQuery, cwits);
		clearWitnessesFromQueryPlan(rootQuery);
		final long t3 = System.nanoTime();
		MonitorFactory.add("constructLeafMaps", "ms", (t1 - t0)/MILLION);
		MonitorFactory.add("populateLeafWitnesses", "ms", (t2 - t1)/MILLION);
		MonitorFactory.add("percolate", "ms", (t3 - t2)/MILLION);
	}
	
	/**
	 * This method does not depend on restricted query classes.
	 * @param doc
	 * @param entDoc
	 */
	void populateLeafWitnesses(IAnnotatedDocument doc, TermProcessor stemTermProcessor) {
		final int docId = doc.docidAsInt();
		final MutableString token = new MutableString();
		final MutableInt pos = new MutableInt();
		
		// single stem token literal witnesses, phrase nodes will do the coalescing
		for (doc.reset(); doc.nextWordToken(pos, token); ) {
			if (!stemTermProcessor.processTerm(token)) continue;
			if (!stemToMatcher.containsKey(token)) continue;
			for (TokenLiteralQuery tokenLiteral : stemToMatcher.get(token)) {
				final Interval posInterval = Interval.valueOf(pos.intValue());
				TokenLiteralWitness witness = new TokenLiteralWitness(docId, posInterval, tokenLiteral);
				tokenLiteral.witnesses.add(witness);
			}
		}
		
		// entity literal and type binding witnesses
		for (Annotation annot : doc.getReferenceAnnotations()) {
			populateEntityAndTypeWitness(doc.docidAsInt(), annot.interval.left, annot.interval.right, annot.entName);
		}
		
		/*
		 * Given getReferenceAnnotations, the following comment no longer holds:
		 * entity literal witnesses, these can span multiple positions and we must coalesce here
		 * type binding witnesses, these can also span multiple positions but that is harder to catch
		 */
	}
	
	void populateEntityAndTypeWitness(final int docId, int beginPos, int endPos, final String entName) {
		final long t0 = System.nanoTime();
		final Interval posInterval = Interval.valueOf(beginPos, endPos);
		// do entity literal matches
		if (entNameToMatcher.containsKey(entName)) {
			for (EntityLiteralQuery entityLiteral : entNameToMatcher.get(entName)) {
				EntityLiteralWitness elw = new EntityLiteralWitness(docId, posInterval, entityLiteral);
				entityLiteral.witnesses.add(elw);
			}
		}
		// do type binding matches
		final int entId = catalog.entNameToEntID(entName.toString());
		if (entId < 0) return;
		final TIntHashSet catIds = new TIntHashSet();
		catalog.catsReachableFromEnt(entId, catIds);
		catIds.forEach(new TIntProcedure() {
			@Override
			public boolean execute(int catId) {
				final String catName = catalog.catIDToCatName(catId);
				if (typeNameToMatcher.containsKey(catName)) {
					for (TypeBindingQuery binding : typeNameToMatcher.get(catName)) {
						final TypeBindingWitness tbw = new TypeBindingWitness(docId, posInterval, binding, entId);
						binding.witnesses.add(tbw);
					}
				}
				return true;
			}
		});
		final long t1 = System.nanoTime();
		MonitorFactory.add("populateEntityAndTypeWitness", "ms", (t1-t0)/MILLION);
	}
}
