package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TObjectIntIterator;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Spotter.ContextRecord;
import iitb.CSAW.Spotter.MentionTrie;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Aggregates contexts into single bags, trims bags by TFIDF, and stores bags
 * compactly in RAM for classifying contexts of phrase matches in payload
 * corpus.
 * 
 * @author soumen
 */
public class ContextFilter {
	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final ACatalog catalog;
	final TokenCountsReader refTcr;
	final TermProcessor termProcessor;
	final MentionTrie trie;
	final float retainNorm;
	final long nDocs;
	
	final ObjectArrayList<String> stems = new ObjectArrayList<String>(200000);
	final FloatArrayList tfidfs = new FloatArrayList(200000);
	final IntArrayList tfidfSorter = new IntArrayList(200000);

	ContextFilter(Config conf) throws Exception {
		this.conf = conf;
		AStripeManager.construct(conf);
		catalog = ACatalog.construct(conf);
		refTcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		nDocs = refTcr.globalDocumentFrequency();
		termProcessor = DefaultTermProcessor.construct(conf);
		trie = MentionTrie.getInstance(conf);
		retainNorm = conf.getFloat(iitb.CSAW.Spotter.PropertyKeys.contextRetainNormName);
	}
	
	void close() throws IOException {
		refTcr.close();
	}
	
	/**
	 * Run should be in increasing order of entName or phrase. Collect the whole
	 * stem to count map for each entName and truncate the map.   
	 */
	void aggregateAndFilterContextsSlow(File inContextFile, File outContextFile, boolean doCheckCatalog) throws InstantiationException, IllegalAccessException, IOException, ClassNotFoundException {
		final DataInputStream contextDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(inContextFile)));
		final DataOutputStream mergedDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(outContextFile)));
		ContextRecord runCr = null;
		long lastUpdate = System.currentTimeMillis();
		ProgressLogger pl = new ProgressLogger(logger);
		pl.logInterval = ProgressLogger.TEN_SECONDS;
//		pl.displayFreeMemory = true;
		pl.start("Started aggregating context bags from " + inContextFile + " to " + outContextFile);
		for (;;) {
			final ContextRecord cr = new ContextRecord();
			try {
				cr.load(contextDis);
				pl.update();
			}
			catch (EOFException eofx) {
				break;
			}
			if (doCheckCatalog && catalog.entNameToEntID(cr.entNameOrPhrase) < 0) {
				continue; // not in our catalog
			}
			if (runCr == null) {
				runCr = cr;
			}
			else {
				final int cmp = runCr.entNameOrPhrase.compareTo(cr.entNameOrPhrase);
				if (cmp == 0) { // accumulate
					for (TObjectIntIterator<String> cx = cr.stemToCount.iterator(); cx.hasNext(); ) {
						cx.advance();
						runCr.stemToCount.adjustOrPutValue(cx.key(), cx.value(), cx.value());
					}
				}
				else if (cmp > 0) {
					throw new IllegalStateException("Input run not sorted");
				}
				else {
					storeOneTruncatedBag(mergedDos, runCr);
					runCr = cr;
				}
			}
			final long now = System.currentTimeMillis();
			if (now > lastUpdate + pl.logInterval) {
				Monitor allStemsMonitor = MonitorFactory.getMonitor("AllStems", null);
				Monitor retainPrefixMonitor = MonitorFactory.getMonitor("RetainPrefix", null);
				logger.info(" AllStems " + allStemsMonitor.getMin() + "/" + allStemsMonitor.getAvg() + "/" + allStemsMonitor.getMax() + " RetainPrefix " + retainPrefixMonitor.getMin() + "/" + retainPrefixMonitor.getAvg() + "/" + retainPrefixMonitor.getMax());
				lastUpdate = now;
			}
		} // for
		if (runCr != null) {
			storeOneTruncatedBag(mergedDos, runCr);
		}
		contextDis.close();
		mergedDos.close();
		pl.stop("Finished aggregating context bags from " + inContextFile + " to " + outContextFile);
		pl.done();
	}

	/**
	 * @param mergedDos
	 * @param runEntName
	 * @param runBag
	 * @throws IOException
	 */
	void storeOneTruncatedBag(DataOutputStream mergedDos, ContextRecord runCr) throws IOException {
		stems.clear();
		tfidfs.clear();
		tfidfSorter.clear();

		// collect idf to form tfidf vector
		float l1norm = 0;
		for (TObjectIntIterator<String> rbx = runCr.stemToCount.iterator(); rbx.hasNext(); ) {
			rbx.advance();
			final String stem = rbx.key();
			final int rawCount = rbx.value();
			final long docFreq = refTcr.globalDocumentFrequency(stem, true);
			if (docFreq == 0) {
				throw new IllegalArgumentException(stem + " has zero doc freq");
			}
			final float comb = (float)rawCount * (float)nDocs / (float)docFreq;
			stems.add(stem);
			tfidfs.add(comb);
			tfidfSorter.add(tfidfSorter.size());
			l1norm += comb;
		}
		
		// sort keys in decreasing order of tfidf weight
		Sorting.quickSort(tfidfSorter.elements(), 0, tfidfSorter.size(), new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final float diff = tfidfs.getFloat(o2) - tfidfs.getFloat(o1);  // note o2 minus o1
				return diff < 0? -1 : (diff > 0? 1 : 0);
			}
		});
		
		// find prefix with large fraction of total weight
		int rx = 0;
		float nOccPrefix = 0, prevToAdd = Float.MAX_VALUE;
		for (; rx < tfidfSorter.size(); ++rx) {
			final float toAdd = tfidfs.get(tfidfSorter.getInt(rx));
			
			if (toAdd > prevToAdd) {
				throw new IllegalStateException(toAdd + " " + prevToAdd);
			}
			prevToAdd = toAdd;
			
			nOccPrefix += toAdd;
			if (nOccPrefix >= retainNorm * l1norm) {
				break;
			}
		}
		MonitorFactory.add("RetainPrefix", null, rx+1);
//		MonitorFactory.add("RetainPrefix", null, runCr.stemToCount.size());
		MonitorFactory.add("AllStems", null, runCr.stemToCount.size());
		
		// we can just shave the rest off runCr itself, no one wants it afterwards
		for (++rx; rx < tfidfSorter.size(); ++rx) {
			runCr.stemToCount.remove(stems.get(tfidfSorter.getInt(rx)));
		}
		runCr.store(mergedDos);
	}
}
