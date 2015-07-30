package iitb.CSAW.Corpus.Webaroo;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Index.AnnotationLeaf;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Index.SIP2.Sip2IndexWriter;
import iitb.CSAW.Spotter.ContextRecordCompact;
import iitb.CSAW.Spotter.DocumentAnnotator;
import iitb.CSAW.Spotter.MentionTrie;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;

public class SipIndexBuilder {
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		SipIndexBuilder aib = new SipIndexBuilder(config);
		aib.run();
	}
	
	/**
	 * If true, choose only top scoring entity unless NA.
	 * If false, include top entities up to and excluding NA.
	 */
	final boolean top1 = true;
	final boolean stopBeforeNa = true;
	final boolean loo = false;
	
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final WebarooStripeManager stripeManager;
	final int nThreads;
	final File corpusBaseDir;
	final ACatalog catalog;
	final TermProcessor termProcessor;
	final MentionTrie mentionTrie;
	final TokenCountsReader tcr;
	
	final IntOpenHashSet catIdSubset;
	
	final AtomicInteger nDocsDone = new AtomicInteger();
	final AtomicLong nAnnotsDone = new AtomicLong();
	final AtomicInteger sharedBatchCounter = new AtomicInteger();
	
	SipIndexBuilder(Config config) throws Exception {
		this.config = config;
		stripeManager = new WebarooStripeManager(config);
		nThreads = config.getInt(Config.nThreadsKey);
		final URI corpus1Uri = stripeManager.corpusDir(stripeManager.myDiskStripe());
		assert corpus1Uri.getHost().equals(stripeManager.myHostName());
		corpusBaseDir = new File(corpus1Uri.getPath());
		if (config.containsKey("TypeSubsetFile")) {
			catIdSubset = new IntOpenHashSet();
			ACatalog catalog = ACatalog.construct(config);
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
		
		catalog = ACatalog.construct(config);
		termProcessor = DefaultTermProcessor.construct(config);
		mentionTrie = MentionTrie.getInstance(config);
		tcr = new TokenCountsReader(new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
	}

	void run() throws Exception {
		nDocsDone.set(0);
		nAnnotsDone.set(0);
		sharedBatchCounter.set(0);
		// single RAR format
		if (!WebarooCorpus.isReady(corpusBaseDir)) {
			return;
		}
		final WebarooCorpus corpus = new WebarooCorpus(config, corpusBaseDir, false, false);
		corpus.reset();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments() / stripeManager.buddyHostStripes(stripeManager.myDiskStripe()).size(); // approximate
		pl.logInterval = ProgressLogger.ONE_MINUTE * 2;
		pl.start();
		
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		for (int tx = 0; tx < nThreads; ++tx) {
			workerPool.add(new IWorker() {
				long wNumDone = 0; // perfect value not critical
				final DocumentAnnotator annotator = new DocumentAnnotator(config, tcr);
				
				@Override
				public long numDone() {
					return wNumDone;
				}
				
				@Override
				public Exception call() throws Exception {
					try {
						Sip2IndexWriter ssiw = new Sip2IndexWriter(config, sharedBatchCounter, catIdSubset);
						final WebarooDocument doc = new WebarooDocument();
						ByteArrayList bal = new ByteArrayList();
						ReferenceArrayList<String> stems = new ReferenceArrayList<String>();
						ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
						ReferenceArrayList<AnnotationLeaf> lannots = new ReferenceArrayList<AnnotationLeaf>();
						for (; corpus.nextDocument(doc, bal); ) {
							if (!stripeManager.isMyJob(doc.docidAsLong())) {
								continue;
							}
							try {
								annotator.processAllTerms(doc, stems);
								MonitorFactory.add("Stems/Doc", null, stems.size());
								annotator.scanMaximal(stems, spots);
								MonitorFactory.add("Spots/Doc", null, spots.size());
								selectAnnotsForSpots(doc, stems, spots, lannots);
								nAnnotsDone.addAndGet(lannots.size());
								ssiw.indexOneDocument(doc.docidAsInt(), lannots);
								pl.update();
								nDocsDone.incrementAndGet();
								wNumDone++;
								Thread.yield();
							}
							catch (Error err) {
								logger.error(Thread.currentThread() + " error " + err);
								err.printStackTrace();
							}
						}
						ssiw.close();
					}
					catch (AssertionError ae) {
						ae.printStackTrace();
						logger.fatal(ae);
						System.exit(-1);
					}
					catch (OutOfMemoryError oom) {
						oom.printStackTrace();
						logger.fatal(oom);
						System.exit(-1);
					}
					return null;
				}
				
				/**
				 * Selects a subset of annotations for each given spot.
				 * Implements some recall-precision balancing policy.
				 */
				protected void selectAnnotsForSpots(WebarooDocument idoc, List<String> stems, List<Spot> spots, List<AnnotationLeaf> annots) throws IOException {
					TIntIntHashMap obag = new TIntIntHashMap();
					annotator.pickSalient(idoc, obag);
					annots.clear();
					final IntArrayList sortedEntIds = new IntArrayList();
					final ContextRecordCompact crc = new ContextRecordCompact();
//					final ContextRecord cr = new ContextRecord();
					TIntFloatHashMap scores = new TIntFloatHashMap();
					int nCandEnt = 0;
					for (Spot spot : spots) {
						nCandEnt += spot.entIds.size();
//						annotator.collectContextAroundSpot(idoc, stems, spot, cr);
						annotator.collectCompactContextAroundSpot(idoc.docidAsLong(), stems, spot, Spot.unknownEnt, obag, crc);
						annotator.classifyContext(crc, spot.entIds, loo, scores);
						annotator.normalizeAndSortScores(scores, sortedEntIds);
						for (int rank = 0, rn = sortedEntIds.size(); rank < rn; ++rank) {
							final int ent = sortedEntIds.getInt(rank);
							final float score = scores.get(ent);
							if (stopBeforeNa && ent == Spot.naEnt) break;
							annots.add(new AnnotationLeaf(catalog.entIDToEntName(ent), spot.span, score, rank, spot.trieLeafNodeId));
							if (top1 && rank >= 0) break;
						}
					}
				}
			});
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		corpus.close();
		logger.info("Completed.");
	}
}
