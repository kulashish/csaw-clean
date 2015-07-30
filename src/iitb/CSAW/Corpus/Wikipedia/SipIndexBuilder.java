package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.AnnotationLeaf;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Index.SIP2.Sip2IndexWriter;
import iitb.CSAW.Spotter.DocumentAnnotator;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.PropertyKeys;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FileLinesCollection;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

public class SipIndexBuilder {
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		SipIndexBuilder sib = new SipIndexBuilder(config);
		sib.run();
	}
	
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	/** Token counts collected on the reference, not payload corpus, because
	 * this will be used by {@link DocumentSpotter} and {@link DocumentAnnotator}.
	 */
	final TokenCountsReader refTcr;
	final int nThreads;
	final AStripeManager stripeManager;
	final IntOpenHashSet catIdSubset;
	final AtomicInteger sharedBatchCounter = new AtomicInteger(0);
	final HashSet<String> discardedMentions; 
	final Monitor fracSpotMissing = MonitorFactory.getMonitor("FracSpotMissing", null);
	
	@SuppressWarnings("unchecked")
	SipIndexBuilder(Config config) throws Exception {
		this.config = config;
		this.refTcr = new TokenCountsReader(new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		stripeManager = AStripeManager.construct(config);
		nThreads = config.getInt(Config.nThreadsKey);
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
		final File discardedMentionsFile = new File(config.getString(PropertyKeys.discardedMentionsFileKey));
		discardedMentions = (HashSet<String>) (discardedMentionsFile.canRead()? BinIO.loadObject(discardedMentionsFile) : null);
		logger.info("Loaded " + (discardedMentions == null? " no " : discardedMentions.size()) + " discarded mentions");
	}
	
	void run() throws Exception {
		final BarcelonaCorpus wCorpus = new BarcelonaCorpus(config);
		wCorpus.reset();
		final ProgressLogger pl = new ProgressLogger();
		pl.expectedUpdates = wCorpus.numDocuments();
		pl.displayFreeMemory = true;
		pl.logInterval = ProgressLogger.ONE_MINUTE/2;
		pl.start();
		final int numProc = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		final WorkerPool workerPool = new WorkerPool(this, numProc);
		for (int px = 0; px < numProc; ++px) {
			workerPool.add(new IWorker() {
				final PhraseWriter pw = new PhraseWriter();
				final DocumentSpotter spotter = new DocumentSpotter(config, refTcr);
				long wNumDone = 0;
				
				@Override
				public Exception call() throws Exception {
					try {
						final Sip2IndexWriter ssiw = new Sip2IndexWriter(config, sharedBatchCounter, catIdSubset);
						final BarcelonaDocument idoc = new BarcelonaDocument();
						while (wCorpus.nextDocument(idoc)) {
							ReferenceArrayList<AnnotationLeaf> lannots = new ReferenceArrayList<AnnotationLeaf>();
							rediscoverLeaves(idoc, lannots);
							ssiw.indexOneDocument(idoc.docidAsInt(), lannots);
							pl.update();
							++wNumDone;
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
					catch (Exception otherEx) {
						otherEx.printStackTrace();
						throw otherEx;
					}
					return null;
				}
				
				/** 
				 * {@link Annotation} does not include the leaf of the
				 * {@link Spot} so we have to rediscover them.
				 * @param annots
				 * @param lannots
				 * @throws IOException 
				 */
				void rediscoverLeaves(BarcelonaDocument bdoc, ReferenceArrayList<AnnotationLeaf> lannots) throws IOException {
					final ReferenceArrayList<String> stems = new ReferenceArrayList<String>();
					final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
					spotter.processAllTerms(bdoc, stems);
					lannots.clear();
					for (Annotation annot : bdoc.getReferenceAnnotations()) {
						final List<String> segment = stems.subList(annot.interval.left, annot.interval.right+1); 
						spots.clear();
						spotter.scanMaximal(segment, spots);
						int nMatch = 0;
						for (final Spot spot : spots) {
							if (spot.span.left == 0 && spot.span.right+1 == segment.size()) {
								final AnnotationLeaf annotl = new AnnotationLeaf(annot.entName, annot.interval, annot.score, annot.rank, spots.get(0).trieLeafNodeId);
								lannots.add(annotl);
								++nMatch;
							}
						}
						if (nMatch > 1) {
							throw new IllegalArgumentException(segment + " leads to " + nMatch + " spots");
						}
						double isMissing = 0;
						if (nMatch < 1 && discardedMentions != null) {
							final String phrase = pw.makePhrase(segment);
							if (!discardedMentions.contains(phrase)) {
								logger.warn("No spot found for " + segment);
								isMissing = 1;
							}
						}
						fracSpotMissing.add(isMissing);
					}
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
				logger.info(fracSpotMissing);
			}
		});
		wCorpus.close();
	}
}
