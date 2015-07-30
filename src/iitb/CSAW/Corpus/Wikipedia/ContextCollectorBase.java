package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Spotter.ContextRecord;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.MemoryStatus;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.Reference2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.util.Interval;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.jamonapi.MonitorFactory;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

/**
 * To make testing on single documents easier.
 * To be used by a single thread only. 
 * @author soumen
 */
public class ContextCollectorBase {
	public static void main(String[] args) throws Throwable {
		final Config conf = new Config(args[0], args[1]);
		ContextCollectorBase ccb = new ContextCollectorBase(conf);
		for (String docIdS : Arrays.copyOfRange(args, 2, args.length)) {
			ccb.collectContextOneDoc(Long.parseLong(docIdS));
		}
		ccb.close();
	}
	
	final Logger logger = Logger.getLogger(getClass());
	final MemoryStatus memoryStatus = new MemoryStatus();
	final Config conf;
	final AStripeManager stripeManager;
	final ACatalog catalog;
	final BarcelonaCorpus corpus;
	final int contextWindow;
	final TokenCountsReader tcr;
	final DocumentSpotter spotter;
	
	ContextCollectorBase(Config conf) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, EnvironmentLockedException, DatabaseException, InstantiationException, IOException, ConfigurationException  {
		this.conf = conf;
		stripeManager = AStripeManager.construct(conf);
		catalog = ACatalog.construct(conf);
		corpus = new BarcelonaCorpus(conf);
		contextWindow = conf.getInt(iitb.CSAW.Spotter.PropertyKeys.contextWindowName);
		tcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		spotter = new DocumentSpotter(conf, tcr);
	}

	void close() throws IOException, DatabaseException {
		tcr.close();
		corpus.close();
	}
	
	/**
	 * Test harness running on a single doc.
	 * @param docId
	 * @throws Exception
	 */
	void collectContextOneDoc(long docId) throws Exception {
		final BarcelonaDocument doc = new BarcelonaDocument();
		if (!corpus.getDocument(docId, doc)) {
			return;
		}
		final ReferenceArrayList<String> stems = new ReferenceArrayList<String>();
		final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>(), negSpots = new ReferenceArrayList<Spot>();
		final Reference2ReferenceOpenHashMap<Annotation, Spot> posGaSpot = new Reference2ReferenceOpenHashMap<Annotation, Spot>();
		final ReferenceArrayList<Annotation> groundAnnots = new ReferenceArrayList<Annotation>();
		
		HashMap<String, ContextRecord> posPhraseToStemToCount = new HashMap<String, ContextRecord>();
		HashMap<String, ContextRecord> negPhraseToStemToCount = new HashMap<String, ContextRecord>();
		
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
			recordOneContext(doc, stems, pgax.getKey().interval, pgax.getKey().entName, pgax.getValue(), posPhraseToStemToCount);
		}
		// record contexts of neg/na phrases
		final PhraseWriter phraseWriter = new PhraseWriter();
		for (Spot spot : negSpots) {
			final String phrase = phraseWriter.makePhrase(stems.subList(spot.span.left, spot.span.right+1));
			recordOneContext(doc, stems, spot.span, phrase, spot, negPhraseToStemToCount);
		}
	}

	/**
	 * Remove ground annot unless corroborated by a spot
	 * @param groundAnnots
	 * @param spots
	 */
	protected void corroborateOrRemove(List<Annotation> groundAnnots, List<Spot> spots) {
		MonitorFactory.add("GroundAnnots", null, groundAnnots.size());
		int nGaEntKnown = 0, nGaSpanSub = 0, nGaSpanEq = 0, nGaEntInLeaf = 0;
		for (Iterator<Annotation> gax = groundAnnots.iterator(); gax.hasNext(); ) {
			final Annotation ga = gax.next();
			boolean isSpanSub = false, isSpanEq = false, isEntInLeaf = false, isVerified = false;
			final int gaEntId = catalog.entNameToEntID(ga.entName);
			if (gaEntId >= 0) {
				++nGaEntKnown;
				for (Spot spot : spots) {
					if (!spot.span.contains(ga.interval)) {
						continue;
					}
					isSpanSub = true;
					if (!ga.interval.equals(spot.span)) {
						continue;
					}
					isSpanEq = true;
					if (!spot.entIds.contains(gaEntId)) {
						continue;
					}
					isEntInLeaf = true;
					isVerified = true;
					break;
				}
			}
			if (!isVerified) {
				gax.remove();
			}
			if (isSpanSub) ++nGaSpanSub;
			if (isSpanEq) ++nGaSpanEq;
			if (isEntInLeaf) ++nGaEntInLeaf;
		} // for-ga
		MonitorFactory.add("GaEntKnown", null, nGaEntKnown);
		MonitorFactory.add("GaSpanSub", null, nGaSpanSub);
		MonitorFactory.add("GaSpanEq", null, nGaSpanEq);
		MonitorFactory.add("GaEntInLeaf", null, nGaEntInLeaf);
		MonitorFactory.add("SupportedGroundAnnots", null, groundAnnots.size());
	}
	
	/**
	 * Optional -- spots that have exactly matching ground annot 
	 * phrases in same doc are added in as ground annot.
	 * <b>Note:</b> When we contruct {@link Annotation#Annotation(String, Interval, float, int)}
	 * there is no check for consistency of the score and rank against other {@link Annotation}s
	 * we might impute.
	 * @param groundAnnots
	 * @param spots
	 */
	protected void makeSpotGroundAnnotIfPhraseMatch(List<String> stems, List<Annotation> groundAnnots, List<Spot> spots) {
		final PhraseWriter phraseWriter = new PhraseWriter();
		final HashMap<String, Annotation> phraseToAnnot = new HashMap<String, Annotation>();
		for (Annotation ga : groundAnnots) {
			final String phraseString = phraseWriter.makePhrase(stems.subList(ga.interval.left, ga.interval.right+1));
			if (phraseToAnnot.containsKey(phraseString)) {
				final Annotation oldGa = phraseToAnnot.get(phraseString);
				if (oldGa != null && !oldGa.entName.equals(ga.entName)) {
					phraseToAnnot.put(phraseString, null); // collision
				}
			}
			else {
				phraseToAnnot.put(phraseString, ga);
			}
		}
		int nImputed = 0;
		for (Spot spot : spots) {
			final String phraseString = phraseWriter.makePhrase(stems.subList(spot.span.left, spot.span.right+1));
			if (phraseToAnnot.containsKey(phraseString)) {
				final Annotation matchAnnot = phraseToAnnot.get(phraseString);
				if (matchAnnot != null) {
					// conjure up a new imputed annotation
					final Annotation newAnnot = new Annotation(matchAnnot.entName, spot.span, 0, 0);
					groundAnnots.add(newAnnot);
					++nImputed;
				}
			}
		}
		MonitorFactory.add("ImputedGroundAnnots", null, nImputed);
	}

	/**
	 * Any spot that has a single token overlap with a ground annot 
	 * is positive, otherwise negative.
	 * @param groundAnnots2
	 * @param spots2
	 */
	protected void separatePosNegSpots(ReferenceArrayList<Annotation> groundAnnots2, ReferenceArrayList<Spot> spots2, Map<Annotation,Spot> posGaSpot, List<Spot> negSpots) {
		MonitorFactory.add("Spots", null, spots2.size());
		posGaSpot.clear();
		negSpots.clear();
		for (Spot spot : spots2) {
			boolean isGround = false;
			for (Annotation ga : groundAnnots2) {
				if (ga.interval.equals(spot.span)) {
					posGaSpot.put(ga, spot);
				}
				if (Annotation.overlaps(ga.interval, spot.span)) {
					isGround = true; // overlaps but not equal means positive, but not recorded
					break;
				}
			}
			if (!isGround) {
				negSpots.add(spot);
			}
		}
		MonitorFactory.add("PosGaSpots", null, posGaSpot.size());
		MonitorFactory.add("NegSpots", null, negSpots.size());
	}
	
	/**
	 * 
	 * @param stems
	 * @param span at the center of the context
	 * @param entNameOrPhrase entName for pos, phrase for neg
	 * @param trieLeafNodeId
	 * @param phraseToCr
	 * @param counts
	 */
	protected void recordOneContext(IDocument doc, List<String> stems, Interval span, String entNameOrPhrase, Spot spot, Map<String, ContextRecord> phraseToCr) {
		final ContextRecord cr;
		if (phraseToCr.containsKey(entNameOrPhrase)) {
			cr = phraseToCr.get(entNameOrPhrase);
		}
		else {
			cr = new ContextRecord();
			phraseToCr.put(entNameOrPhrase, cr);
		}
		spotter.collectContextAroundSpot(doc, stems, spot, cr);
		cr.entNameOrPhrase = entNameOrPhrase;
	}

	static DataOutputStream getBufferedDos(File file) throws FileNotFoundException {
		return new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(file)));
	}
}
