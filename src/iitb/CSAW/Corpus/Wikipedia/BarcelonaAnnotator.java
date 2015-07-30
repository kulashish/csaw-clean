package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Spotter.ContextRecordCompact;
import iitb.CSAW.Spotter.DocumentAnnotator;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.logging.ProgressLogger;

import java.util.Map;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Runs the spotter and disambiguator back on the reference corpus itself.
 * For debugging and profiling.  Extends {@link ContextCollectorBase} so that
 * we can prune and impute ground annotations exactly as in 
 * {@link ContextCollector}.
 * 
 * @author soumen
 */
public class BarcelonaAnnotator extends ContextCollectorBase {
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final BarcelonaAnnotator ba = new BarcelonaAnnotator(conf);
		ba.annotate();
		ba.close();
	}

	final boolean loo = false;
	final Monitor leafSmallMon = MonitorFactory.getMonitor("LeafSmall", null);
	final Monitor accuracyMon = MonitorFactory.getMonitor("Accuracy", null);
	
	BarcelonaAnnotator(Config conf) throws Exception {
		super(conf);
	}

	void annotate() throws Exception {
		corpus.reset();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.logInterval = ProgressLogger.ONE_MINUTE/2;
		pl.start("Started annotating.");
		WorkerPool wp = new WorkerPool(this, conf.getInt(Config.nThreadsKey));
		for (int tx = 0; tx < conf.getInt(Config.nThreadsKey); ++tx) {
			wp.add(new IWorker() {
				long tNumDone = 0;
				@Override
				public Exception call() throws Exception {
					final DocumentAnnotator annotator;
					annotator = new DocumentAnnotator(conf, tcr);
					final BarcelonaDocument doc = new BarcelonaDocument();
					final ReferenceArrayList<String> stems = new ReferenceArrayList<String>();
					final ReferenceArrayList<Annotation> groundAnnots = new ReferenceArrayList<Annotation>();
					final Object2ObjectOpenHashMap<Annotation, Spot> posGaSpot = new Object2ObjectOpenHashMap<Annotation, Spot>();
					final ReferenceArrayList<Spot> negSpots = new ReferenceArrayList<Spot>();
					final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
					long numSpots = 0, numNaSpots = 0;
					while (corpus.nextDocument(doc)) {
						pl.update();
						++tNumDone;
						TIntIntHashMap obag = new TIntIntHashMap();
						annotator.pickSalient(doc, obag);
						groundAnnots.clear();
						groundAnnots.addAll(doc.getReferenceAnnotations());
						annotator.processAllTerms(doc, stems);
						MonitorFactory.add("NumStems", null, stems.size());
						annotator.scanMaximal(stems, spots);
						corroborateOrRemove(groundAnnots, spots);
						makeSpotGroundAnnotIfPhraseMatch(stems, groundAnnots, spots);
						separatePosNegSpots(groundAnnots, spots, posGaSpot, negSpots);
						numSpots += (posGaSpot.size() + negSpots.size());
						numNaSpots += negSpots.size();
						final TIntFloatHashMap scores = new TIntFloatHashMap();
						final IntArrayList sortedEntIds = new IntArrayList();
						final ContextRecordCompact crc = new ContextRecordCompact();
						try {
							// separately process pos and neg spots
							for (Map.Entry<Annotation, Spot> pgax : posGaSpot.entrySet()) {
								final Annotation annot = pgax.getKey();
								final Spot spot = pgax.getValue();
								if (annotator.numTrainingContexts(spot.trieLeafNodeId) <= 1) {
									leafSmallMon.add(1);
									continue;
								}
								leafSmallMon.add(0);
								final int entId = catalog.entNameToEntID(annot.entName);
								assert entId >= 0;
								spotter.collectCompactContextAroundSpot(doc.docidAsLong(), stems, spot, entId, obag, crc);
								scores.clear();
								annotator.classifyContext(crc, spot.entIds, loo, scores);
								if (!scores.isEmpty()) {
									annotator.normalizeAndSortScores(scores, sortedEntIds);
								}
								final int rank = sortedEntIds.indexOf(entId);
								accuracyMon.add(rank == 0? 1 : 0);
							}
							for (Spot spot : negSpots) {
								if (annotator.numTrainingContexts(spot.trieLeafNodeId) <= 1) {
									leafSmallMon.add(1);
									continue;
								}
								leafSmallMon.add(0);
								spotter.collectCompactContextAroundSpot(doc.docidAsLong(), stems, spot, Spot.naEnt, obag, crc);
								scores.clear();
								annotator.classifyContext(crc, spot.entIds, loo, scores);
								if (scores.isEmpty()) {
									accuracyMon.add(1); // technically this is correct
								}
								else {
									annotator.normalizeAndSortScores(scores, sortedEntIds);
									final int naRank = sortedEntIds.indexOf(Spot.naEnt);
									accuracyMon.add(naRank == 0? 1 : 0);
								}
							}
						}
						catch (IllegalStateException isx) {
							logger.error("doc=" + doc.docidAsLong() + " threw " + isx.getMessage());
						}
					}
					return null;
				}
				
				@Override
				public long numDone() {
					return tNumDone;
				}
			});
		}
		wp.pollToCompletion(ProgressLogger.ONE_MINUTE, new Runnable() {
			@Override
			public void run() {
				logger.info(leafSmallMon);
				logger.info(accuracyMon);
			}
		});
		pl.stop("Finished annotating.");
		pl.done();
		logger.info(leafSmallMon);
		logger.info(accuracyMon);
	}
}
