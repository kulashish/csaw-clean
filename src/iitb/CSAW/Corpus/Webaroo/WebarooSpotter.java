package iitb.CSAW.Corpus.Webaroo;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Spotter.ContextRecordCompact;
import iitb.CSAW.Spotter.DocumentAnnotator;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.MentionTrie;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.log4j.Logger;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;

/**
 * Test harness for spotting and annotating Webaroo docs.
 */
public class WebarooSpotter {
	/**
	 * @param args [0]=config [1]=log [2..]=docids
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		TokenCountsReader refTcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		WebarooSpotter webarooSpotter = new WebarooSpotter(conf, refTcr);
		if (2 < args.length) {
			for (int ac = 2; ac < args.length; ++ac) {
				webarooSpotter.doAnnot(Long.parseLong(args[ac]));
			}
		}
		else {
			webarooSpotter.doAnnot();
		}
		webarooSpotter.close();
		
		webarooSpotter.logger.info(MonitorFactory.getMonitor("NumTokens", null));
		webarooSpotter.logger.info(MonitorFactory.getMonitor("NumSpotsMax", null));
		webarooSpotter.logger.info(MonitorFactory.getMonitor("AmbiguityMax", null));
		webarooSpotter.logger.info(MonitorFactory.getMonitor("NaRankMax", null));
	}
	
	final Logger logger = Logger.getLogger(getClass());
	final boolean loo = false;
	final Config conf;
	final AStripeManager stripeManager;
	final WebarooCorpus corpus;
	final ACatalog catalog;
	final MentionTrie trie;
	/** Token counts over the reference corpus, used by {@link DocumentAnnotator}. */
	final TokenCountsReader refTcr;
	final Monitor featureFillMonitor = MonitorFactory.getMonitor("FeatureFill", null);
	
	WebarooSpotter(Config conf, TokenCountsReader tcr) throws Exception {
		this.conf = conf;
		stripeManager = AStripeManager.construct(conf);
		URI corpusUri = stripeManager.corpusDir(stripeManager.myDiskStripe());
		assert corpusUri.getHost().equals(stripeManager.myHostName());
		corpus = new WebarooCorpus(conf, new File(corpusUri.getPath()), false, false);
		catalog = ACatalog.construct(conf);
		trie = MentionTrie.getInstance(conf);
		this.refTcr = tcr;
	}
	
	void close() throws IOException, DatabaseException {
		corpus.close();
	}
	
	void doAnnot(long docId) throws Exception {
		final IAnnotatedDocument idoc = corpus.allocateReusableDocument();
		final ByteArrayList bal = new ByteArrayList();
		final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
		final ReferenceArrayList<String> tokens = new ReferenceArrayList<String>();
		final Monitor numTokens = MonitorFactory.getMonitor("NumTokens", null);
		final Monitor numSpotsMax = MonitorFactory.getMonitor("NumSpotsMax", null);
		
		if (!corpus.getDocument(docId, idoc, bal)) {
			logger.warn("doc " + docId + " not in stripe");
			return;
		}
		
		logger.info("Spotting doc " + docId);
		final DocumentAnnotator annotator = new DocumentAnnotator(conf, refTcr);
		final TermProcessor termProcessor = DefaultTermProcessor.construct(conf);
		DocumentSpotter.processAllTerms(termProcessor, idoc, tokens);
		TIntFloatHashMap scores = new TIntFloatHashMap();
		annotator.scanMaximal(tokens, spots);
		collectScoreDistribution(annotator, idoc, tokens, spots, scores);
		numSpotsMax.add(spots.size());
		numTokens.add(tokens.size());
	}
	
	void doAnnot() throws Exception {
		corpus.reset();
		final ProgressLogger pl = new ProgressLogger(logger);
//		pl.expectedUpdates = corpus.numDocuments();
		pl.displayFreeMemory = true;
		pl.start();
		final int nThreads = conf.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		for (int tx = 0; tx < nThreads; ++tx) {
			workerPool.add(new IWorker() {
				final DocumentAnnotator annotator = new DocumentAnnotator(conf, refTcr);
				final TermProcessor termProcessor = DefaultTermProcessor.construct(conf);
				long wNumDone = 0;
				
				@Override
				public Exception call() throws Exception {
					final IAnnotatedDocument idoc = corpus.allocateReusableDocument();
					final ByteArrayList bal = new ByteArrayList();
					final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
					final ReferenceArrayList<String> tokens = new ReferenceArrayList<String>();
					for (; corpus.nextDocument(idoc, bal); ) {
						DocumentSpotter.processAllTerms(termProcessor, idoc, tokens);
						annotator.scanMaximal(tokens, spots);
						TIntFloatHashMap scores = new TIntFloatHashMap();
						collectScoreDistribution(annotator, idoc, tokens, spots, scores);
						pl.update();
						++wNumDone;
						Thread.yield();
					}
					return null;
				}
				
				@Override
				public long numDone() {
					return wNumDone;
				}
			});
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, new Runnable() {
			@Override
			public void run() {
				logger.info("Feature fill " + featureFillMonitor);
			}
		});
		pl.stop();
		pl.done();
	}
	
	private void collectScoreDistribution(DocumentAnnotator annotator, IAnnotatedDocument idoc, List<String> stems, List<Spot> spots, final TIntFloatHashMap scores) throws IOException {
		final TIntIntHashMap salientBag = new TIntIntHashMap();
		annotator.pickSalient(idoc, salientBag);

		final Monitor naRankMax = MonitorFactory.getMonitor("NaRankMax", null);
		final Monitor ambMax = MonitorFactory.getMonitor("AmbiguityMax", null);
		final ContextRecordCompact crc = new ContextRecordCompact();
		for (Spot spot : spots) {
			ambMax.add(spot.entIds.size());
			annotator.collectCompactContextAroundSpot(idoc.docidAsLong(), stems, spot, Spot.unknownEnt, salientBag, crc);
			featureFillMonitor.add(crc.nearTermIdToCount.size() + crc.salientTermIdToCount.size());
			scores.clear();
			annotator.classifyContext(crc, spot.entIds, loo, scores);
			if (scores.isEmpty()) {
				continue;
			}
			final IntArrayList sorted = new IntArrayList();
			annotator.normalizeAndSortScores(scores, sorted);
			final int naRank = sorted.indexOf(Spot.naEnt);
			if (naRank < 0) {
				logger.error("NA not found");
				continue;
			}
			naRankMax.add(naRank);
		}
	}
}
