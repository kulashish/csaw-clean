package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Spotter.ContextBase;
import iitb.CSAW.Spotter.ContextRecord;
import iitb.CSAW.Spotter.ContextRecordCompact;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.MentionTrie;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * Scans reference corpus and collects context stem bags near positive 
 * (annotated with entity) and negative (NA) occurrences of phrases in 
 * a given {@link MentionTrie}.  Note that fixing the {@link MentionTrie}
 * also fixes the IDs assigned to its leaf nodes.
 * 
 * @author soumen
 */
public class ContextCollector extends ContextCollectorBase {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2]=doCompact?
	 * @throws Throwable 
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		final boolean doCompact = Boolean.parseBoolean(args[2]);
		ContextCollector mcmb = new ContextCollector(conf, doCompact);
		mcmb.collectContexts();
		mcmb.close();
	}
	
	final int sumHighWaterKeys = 1000000;
	final int nThreads;
	final File ctxDir1, ctxDir2;
	/** Old aggregated context for NB or new separate but compact contexts for LR? */
	final boolean doCompact;
	final File posContextFile, negContextFile;
	
	ContextCollector(Config conf, boolean doCompact) throws Exception {
		super(conf);
		this.doCompact = doCompact;
		nThreads = conf.getInt(Config.nThreadsKey);
		ctxDir1 = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.contextBaseDirKey1));
		ctxDir2 = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.contextBaseDirKey2));	
		if (doCompact) {
			posContextFile = negContextFile = null;
		}
		else {
			posContextFile = new File(ctxDir1, ContextBase.posContextFileName);
			negContextFile = new File(ctxDir1, ContextBase.negContextFileName);
		}
	}
	
	/**
	 * Writes runs for both annotated and unannotated phrase matches to context bags.
	 * In case the phrase is annotated we use the standard entity name, otherwise we
	 * use the phrase string.
	 */
	void collectContexts() throws Exception {
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		corpus.reset();
		final DataOutputStream posContextDos = doCompact? null : new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(posContextFile)));
		final DataOutputStream negContextDos = doCompact? null : new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(negContextFile)));
		
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.displayFreeMemory = true;
		
		pl.start("Started collecting contexts.");
		for (int tx = 0; tx < nThreads; ++tx) {
			workerPool.add(new IWorker() {
				long wNumDone = 0;
				final PhraseWriter phraseWriter = new PhraseWriter(); 
				final DocumentSpotter spotter = new DocumentSpotter(conf, tcr);
				final BarcelonaDocument doc = new BarcelonaDocument();
				final ReferenceArrayList<String> stems = new ReferenceArrayList<String>();
				final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>(), negSpots = new ReferenceArrayList<Spot>();
				final Reference2ReferenceOpenHashMap<Annotation, Spot> posGaSpot = new Reference2ReferenceOpenHashMap<Annotation, Spot>();
				final ReferenceArrayList<Annotation> groundAnnots = new ReferenceArrayList<Annotation>();
				final Object2ObjectOpenHashMap<String, ContextRecord> posPhraseToStemToCount = new Object2ObjectOpenHashMap<String, ContextRecord>();
				final Object2ObjectOpenHashMap<String, ContextRecord> negPhraseToStemToCount = new Object2ObjectOpenHashMap<String, ContextRecord>();
				
				@Override
				public Exception call() throws Exception {
					try {
						final TIntIntHashMap obag = new TIntIntHashMap();
						final DataOutputStream compactContextDos = doCompact? getBufferedDos(new File(ctxDir2, ContextBase.compactContextFileName + Thread.currentThread().getName())) : null;
						final ContextRecordCompact crc = new ContextRecordCompact();
						while (corpus.nextDocument(doc)) {
							pl.update();
							++wNumDone;
							
							// collect whole document features (salient words)
							spotter.pickSalient(doc, obag);
							
							// first collect all reported ground annots
							groundAnnots.clear();
							for (Annotation ga : doc.getReferenceAnnotations()) {
								groundAnnots.add(ga);
							}
							// and all spots reported by the trie
							spotter.processAllTerms(doc, stems);
							spotter.scanMaximal(stems, spots);
							// now process the annots, spots and contexts
							corroborateOrRemove(groundAnnots, spots);
							makeSpotGroundAnnotIfPhraseMatch(stems, groundAnnots, spots);
							separatePosNegSpots(groundAnnots, spots, posGaSpot, negSpots);
							// record contexts of pos/annot mentions
							for (Map.Entry<Annotation, Spot> pgax : posGaSpot.entrySet()) {
								final int entId = catalog.entNameToEntID(pgax.getKey().entName);
								assert entId >= 0;
								if (doCompact) {
									spotter.collectCompactContextAroundSpot(doc.docidAsLong(), stems, pgax.getValue(), entId, obag, crc);
									crc.store(compactContextDos);
								}
								else {
									recordOneContext(doc, stems, pgax.getKey().interval, pgax.getKey().entName, pgax.getValue(), posPhraseToStemToCount);
								}
							}
							// record contexts of neg/na phrases
							for (Spot spot : negSpots) {
								final String phrase = phraseWriter.makePhrase(stems.subList(spot.span.left, spot.span.right+1));
								if (doCompact) {
									spotter.collectCompactContextAroundSpot(doc.docidAsLong(), stems, spot, Spot.naEnt, obag, crc);
									crc.store(compactContextDos);
								}
								else {
									recordOneContext(doc, stems, spot.span, phrase, spot, negPhraseToStemToCount);
								}
							}
							flushAndClear(posPhraseToStemToCount, posContextDos, false);
							flushAndClear(negPhraseToStemToCount, negContextDos, false);
						} // for-doc
						// final compulsory flush
						flushAndClear(posPhraseToStemToCount, posContextDos, true);
						flushAndClear(negPhraseToStemToCount, negContextDos, true);
						if (doCompact) compactContextDos.close();
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
					catch (Exception otherx) {
						logger.warn(otherx);
						throw otherx;
					}
					return null;
				}

				private void flushAndClear(Object2ObjectOpenHashMap<String, ContextRecord> phraseToCr, DataOutputStream dos, boolean force) throws IOException {
					if (doCompact) return;
					if (dos == null) return;
					if (!force && payload(phraseToCr) < sumHighWaterKeys / nThreads) {
						return;
					}
					// write to disk
					synchronized (dos) {
						for (Map.Entry<String, ContextRecord> mscx : phraseToCr.entrySet()) {
							mscx.getValue().store(dos);
						}
					}
					// and clear up
					phraseToCr.clear();
				}
				
				private int payload(Object2ObjectOpenHashMap<String, ContextRecord> map) {
					int ans = 0;
					for (ContextRecord cr : map.values()) {
						ans += cr.stemToCount.size();
					}
					return ans;
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
				final Monitor mSpots = getMonitor("Spots"), 
				mPosGaSpots = getMonitor("PosGaSpots"),
				mNegSpots = getMonitor("NegSpots"),
				mGa = getMonitor("GroundAnnots"),
				mSupportedGa = getMonitor("SupportedGroundAnnots"),
				mImputed = getMonitor("ImputedGroundAnnots");
				logger.info(mSpots.getLabel() + ":" + mSpots.getAvg() + " " + mPosGaSpots.getLabel() + ":" + mPosGaSpots.getAvg() + " " + mNegSpots.getLabel() + ":" + mNegSpots.getAvg() + " " + mGa.getLabel() + ":" + mGa.getAvg() + " " + mSupportedGa.getLabel() + ":" + mSupportedGa.getAvg() + " " + mImputed.getLabel() + ":" + mImputed.getAvg());
			}
		});
		pl.stop("Finished collecting contexts.");
		pl.done();
		if (posContextDos != null) posContextDos.close();
		if (negContextDos != null) negContextDos.close();
		logger.info(MonitorFactory.getMonitor("EntIsCandidate", null));
	}
	
	private Monitor getMonitor(String name) {
		return MonitorFactory.getMonitor(name, null);
	}
}
