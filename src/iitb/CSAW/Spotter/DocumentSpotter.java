package iitb.CSAW.Spotter;

import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.TestDocument;
import iitb.CSAW.Corpus.Wikipedia.SalientWordsPicker;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.util.Interval;
import it.unimi.dsi.mg4j.index.TermProcessor;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;

/**
 * Given a {@link IDocument} and a {@link MentionTrie}, applies the trie
 * to find spots and converts them to {@link Annotation}.  <b>Note</b> that
 * this class is not thread-safe; one instance is needed per thread.
 * @author devshree
 * @author soumen
 * @since 2010/10/20
 */
public class DocumentSpotter {
    final Logger logger = Logger.getLogger(getClass());
	final Config conf;
    final ACatalog catalog;
	// not thread safe
	final MentionTrie trie;
	final MentionTrie.KeyMaker km = new MentionTrie.KeyMaker();
	final TermProcessor tp;
	final int contextWindow;
	/** Token counts collected over reference corpus. */
	final TokenCountsReader refTcr;
	final SalientWordsPicker swp;
    
	/**
	 * @param conf
	 * @param refTcr collected over the reference corpus
	 */
    public DocumentSpotter(Config conf, TokenCountsReader refTcr) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, InstantiationException, ConfigurationException {
    	this.conf = conf;
    	this.trie = MentionTrie.getInstance(conf);
    	this.tp = DefaultTermProcessor.construct(conf).copy();
    	this.catalog = ACatalog.construct(conf);
		contextWindow = conf.getInt(iitb.CSAW.Spotter.PropertyKeys.contextWindowName);
		this.refTcr = refTcr;
		swp = new SalientWordsPicker(conf, refTcr);
    }
    
	public void pickSalient(IDocument doc, TIntIntHashMap obag) {
		swp.pick(doc, obag);
	}
	
    public void scanAny(List<String> doc, List<Spot> spots) throws IOException {
    	IntList leafEntIds = null;
    	spots.clear();
    	IntArrayList path = new IntArrayList(Arrays.asList(MentionTrie.rootNodeId));
    	int cursor = 0;
    	for (;;) {
    		if (cursor >= doc.size() && path.size() <= 1) break;
    		final String token = cursor < doc.size()? doc.get(cursor) : null;
    		++cursor;
    		if (path.size() > 1 && (leafEntIds = trie.getSortedEntsNa(path.topInt())) != null) {
    			final Interval curSpan = Interval.valueOf(cursor - path.size(), cursor-2);
				logger.trace("path " + path + " span " + doc.subList(curSpan.left, curSpan.right+1) + " ents " + leafEntIds.size());
				spots.add(new Spot(path.topInt(), curSpan, leafEntIds));
				leafEntIds = null;
    		}
    		final int nextNode = token == null? path.topInt(): trie.step(km, path.topInt(), token);
    		logger.trace(path.topInt() + "," + token + " --> " + nextNode);
    		if (nextNode == path.topInt()) { // can't extend path any more
    			while (path.size() > 1) {
    				path.popInt();
    				--cursor;
    			}
    		}
    		else {
    			path.push(nextNode); // can extend
    		}
    	}
    }
    
    /**
     * Only maximal annotations are reported.
     * @param doc
     * @param spots
     * @throws IOException
     */
    public void scanMaximal(List<String> doc, List<Spot> spots) throws IOException {
    	IntList leafEntIds = null;
    	spots.clear();
    	IntArrayList path = new IntArrayList(Arrays.asList(MentionTrie.rootNodeId));
    	int cursor = 0;
    	for (;;) {
    		if (cursor >= doc.size() && path.size() <= 1) break;
    		final String token = cursor < doc.size()? doc.get(cursor) : null;
    		++cursor;
    		final int nextNode = token == null? path.topInt(): trie.step(km, path.topInt(), token);
    		logger.trace(path.topInt() + "," + token + " --> " + nextNode);
    		if (nextNode == path.topInt()) { // can't extend path any more
    			// find longest prefix of path that ends in a leaf
    			leafEntIds = null;
    			while (!path.isEmpty() && (leafEntIds = trie.getSortedEntsNa(path.topInt())) == null) {
    				path.pop();
    				--cursor;
    			}
    			if (path.size() > 1 && !leafEntIds.isEmpty()) {
    				// the span is cursor-path.size()..cursor-2
    				Interval curSpan = Interval.valueOf(cursor - path.size(), cursor-2);
    				logger.trace("path " + path + " span " + doc.subList(curSpan.left, curSpan.right+1));
        			// generate spot unless a previously emitted spot's span strictly contains current one
    				boolean isSubsumed = false;
    				for (int pax = spots.size()-1; pax >= 0; --pax) {
    					final Spot spot = spots.get(pax);
    					if (spot.span.right < curSpan.left) break;
    					if (spot.span.contains(curSpan) && !curSpan.contains(spot.span)) {
    						isSubsumed = true;
    						break;
    					}
    				}
    				if (!isSubsumed) {
    					spots.add(new Spot(path.topInt(), curSpan, leafEntIds));
    				}
    			}
    			cursor = cursor - path.size() + 1;
    			path.clear();
    			path.push(MentionTrie.rootNodeId);
    		}
    		else {
    			path.push(nextNode); // can extend
    		}
    	}
    }

    /**
     * Invokes {@link TermProcessor} on all tokens and arranges in array.
     * @param tp term processor
     * @param idoc input token stream
     * @param odoc processed token array. For input tokens that do not pass
     * {@link #processTerm(MutableString)}, we fill corresponding slots
     * with null.
     */
    public static void processAllTerms(TermProcessor tp, IDocument idoc, List<String> odoc) {
    	final MutableString atok = new MutableString();
    	odoc.clear();
    	for (int tx = 0, tn = idoc.numWordTokens(); tx < tn; ++tx) {
    		atok.replace(idoc.wordTokenAt(tx));
    		if (tp.processTerm(atok)) {
    			odoc.add(atok.toString());
    		}
    		else {
    			odoc.add(null);
    		}
    	}
    }

    /**
     * Non-static version of {@link #processAllTerms(TermProcessor, IDocument, List)}
     * @param idoc
     * @param ostems
     */
    public void processAllTerms(IDocument idoc, List<String> ostems) {
    	processAllTerms(tp, idoc, ostems);
    }
    
	/**
	 * @param doc
	 * @param stems
	 * @param spot
	 * @param cr output, will be initially emptied out
	 */
	public void collectContextAroundSpot(IDocument doc, List<String> stems, Spot spot, ContextRecord cr) {
		cr.setNull();
		final int left = Math.max(0, spot.span.left - contextWindow);
		final int right = Math.min(stems.size()-1, spot.span.right + contextWindow);
		for (int pos = left; pos < spot.span.left; ++pos) {
			final String contextToken = stems.get(pos);
			if (contextToken != null) {
				cr.stemToCount.adjustOrPutValue(contextToken, 1, 1);
			}
		}
		for (int pos = spot.span.right + 1; pos <= right; ++pos) {
			final String contextToken = stems.get(pos);
			if (contextToken != null) {
				cr.stemToCount.adjustOrPutValue(contextToken, 1, 1);
			}
		}
	}
	
	/**
	 * @param stems
	 * @param spot
	 * @param entId could be {@link Spot#unknownEnt} for testing/application
	 * @param salientBag
	 * @param crc
	 */
	public void collectCompactContextAroundSpot(long docId, List<String> stems, Spot spot, int entId, TIntIntHashMap salientBag, ContextRecordCompact crc) {
		final IntList candEnts = trie.getSortedEntsNa(spot.trieLeafNodeId);
		assert entId == Spot.unknownEnt || candEnts.contains(entId) : "D" + docId + "@" + spot.span + "=" + stems.subList(spot.span.left, spot.span.right+1) + "_eset" + candEnts + " does not contain E" + entId + "=" + catalog.entIDToEntName(entId);
		if (entId != Spot.naEnt && entId != Spot.unknownEnt) {
			MonitorFactory.add("EntIsCandidate", null, trie.getSortedEntsNa(spot.trieLeafNodeId).contains(entId)? 1 : 0);
		}
		crc.init();
		crc.docId = docId;
		crc.mentionSpan = spot.span;
		crc.trieLeaf = spot.trieLeafNodeId;
		crc.entId = entId;
		final int left = Math.max(0, spot.span.left - contextWindow);
		final int right = Math.min(stems.size()-1, spot.span.right + contextWindow);
		collectOneContextCompactSpan(stems, left, spot.span.left, crc);
		collectOneContextCompactSpan(stems, spot.span.right+1, right+1, crc);
		// salient bag
		crc.salientTermIdToCount.clear();
		crc.salientTermIdToCount.putAll(salientBag);
	}
	
	/**
	 * This is where {@link #refTcr} is used, which is why it has to be collected from the reference corpus.
	 */
	private void collectOneContextCompactSpan(List<String> stems, int begin, int end, ContextRecordCompact crc) {
		for (int pos = begin; pos < end; ++pos) {
			final String contextToken = stems.get(pos);
			if (contextToken == null) {
				continue;
			}
			final long termId = refTcr.mapTermToId(contextToken, false);
			if (termId == -1) {
				continue;
			}
			if (termId > Integer.MAX_VALUE) {
				throw new ArithmeticException();
			}
			crc.nearTermIdToCount.adjustOrPutValue((int) termId, 1, 1);
		}
	}

    /**
     * Test harness.
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	Config conf = new Config(args[0], args[1]);
    	TermProcessor tp = DefaultTermProcessor.construct(conf);
    	final TokenCountsReader tcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
    	DocumentSpotter ds = new DocumentSpotter(conf, tcr);
    	TestDocument td = new TestDocument(1001, "Tendulkar", "brin", "for", "mairzi", "doats", "s", "Bill", "Gates", "went", "gates", "sergey", "brin");
    	ReferenceArrayList<Annotation> annots = new ReferenceArrayList<Annotation>();
    	
//    	ds.logger.setLevel(Level.TRACE);
    	
//    	ds.scanMaximal(td, annots);
//    	System.out.println("ANNOTS " + annots);

    	ReferenceArrayList<String> toks = new ReferenceArrayList<String>();
    	ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
    	DocumentSpotter.processAllTerms(tp, td, toks);
    	ds.scanMaximal(toks, spots);
    	System.out.println("Maximal spots " + spots);

    	spots.clear();
    	annots.clear();
    	ds.scanAny(toks, spots);
    	System.out.println("Any spots " + spots);
    }
}
