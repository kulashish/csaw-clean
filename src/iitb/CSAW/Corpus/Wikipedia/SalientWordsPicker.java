package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TLongFloatHashMap;
import gnu.trove.TLongFloatIterator;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.File;

import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.LongComparator;

/**
 * <p>Uses IDF from the reference corpus to construct a TFIDF vector for the
 * given document, then picks the top K words/stems that contribute most
 * to the norm of the TFIDF vector.  The
 * <a href="http://en.wikipedia.org/wiki/Tf%E2%80%93idf">definition of TFIDF</a>
 * here is from Wikipedia.</p>
 * <p><b>Note:</b> not thread-safe because of member arrays and maps;
 * construct one per thread.</p> 
 * @author soumen
 * @since 2011/05/30
 */
public class SalientWordsPicker {
	final Config conf;
	/** Retain enough terms to get at least this fraction of original document norm. */ 
	final double normFracMin = 0.9;
	/** Retain at most this many terms, even if norm condition is not satisfied. */
	final int termsMax = 10;
	final TermProcessor tp;
	final TokenCountsReader tcr;
	final double nDoc;
	
	/** Not thread safe */
	final LongArrayList termIdSorter = new LongArrayList();
	/** Not thread safe */
	final TLongFloatHashMap tfidfBag = new TLongFloatHashMap();
	
	public SalientWordsPicker(Config conf, TokenCountsReader tcr) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.conf = conf;
		this.tp = DefaultTermProcessor.construct(conf);
		this.tcr = tcr;
		nDoc = tcr.globalDocumentFrequency();
	}
	
	/**
	 * Not thread safe, only one invocation per class instance at a time.
	 * @param idoc Input document.
	 * @param xbag Output bag of salient words. Note the document norm changes.
	 */
	public void pick(IDocument idoc, TIntIntHashMap xbag) {
		xbag.clear();
		tfidfBag.clear();
		final MutableString mterm = new MutableString();
		// collect raw tfs
		for (int wx = 0, wn = idoc.numWordTokens(); wx < wn; ++wx) {
			mterm.replace(idoc.wordTokenAt(wx));
			if (!tp.processTerm(mterm)) continue;
			final long termId = tcr.mapTermToId(mterm, false);
			if (termId < 0) continue;
			tfidfBag.adjustOrPutValue(termId, 1, 1);
			xbag.adjustOrPutValue((int) termId, 1, 1); 
		}
		// scale by idf
		float fullNorm = 0;
		termIdSorter.clear();
		for (TLongFloatIterator ox = tfidfBag.iterator(); ox.hasNext(); ) {
			ox.advance();
			final double nDocTerm = 1d + tcr.globalDocumentFrequency(ox.key()); // Wikipedia definition
			assert nDocTerm > 0;
			final double rawIdf = nDoc / nDocTerm;
			final double logIdf = Math.log(rawIdf);
			final double rawTf = ox.value();
			final double tfIdf = rawTf * logIdf;
			ox.setValue((float) tfIdf);
			fullNorm += tfIdf * tfIdf;
			termIdSorter.add(ox.key());
		}
		// get top contributors to norm
		Sorting.quickSort(termIdSorter.elements(), 0, termIdSorter.size(), new LongComparator() {
			@Override
			public int compare(long o1, long o2) {
				final float f1 = tfidfBag.get(o1), f2 = tfidfBag.get(o2);
				if (f1 > f2) return -1;
				if (f1 < f2) return 1;
				if (o1 > o2) return -1;  // deterministic tie breaking
				if (o1 < o2) return 1;   // deterministic tie breaking
				return 0;
			}
		});
		// remove rest from out bag
		final double targetNorm = fullNorm * normFracMin;
		float partNorm = 0;
		int nTerms = 0;
		for (long termId : termIdSorter) {
			if (partNorm > targetNorm || nTerms > termsMax) {
				xbag.remove((int) termId);
			}
			partNorm += tfidfBag.get(termId) * tfidfBag.get(termId);
			++nTerms;
		}
	}
	
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final TokenCountsReader tcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		final SalientWordsPicker swp = new SalientWordsPicker(conf, tcr);
		final BarcelonaCorpus corpus = new BarcelonaCorpus(conf);
		final BarcelonaDocument bdoc = new BarcelonaDocument();
		final TIntIntHashMap obag = new TIntIntHashMap();
		Logger logger = Logger.getLogger(swp.getClass());
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.logInterval = ProgressLogger.TEN_SECONDS;
		pl.start();
		for (corpus.reset(); corpus.nextDocument(bdoc); ) {
			swp.pick(bdoc, obag);
			logger.debug(bdoc.cleanText);
			for (int termId : obag.keys()) {
				logger.debug(tcr.mapIdToTerm(termId));
			}
			pl.update();
		}
		pl.stop();
		pl.done();
		corpus.close();
		tcr.close();
	}
}
