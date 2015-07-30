package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;
import gnu.trove.TObjectProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Spotter.MentionRecord;
import iitb.CSAW.Spotter.MentionTrie;
import iitb.CSAW.Spotter.PropertyKeys;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.MemoryStatus;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import cern.colt.Sorting;

/**
 * Reads {@link PropertyKeys#mergedMentionsFileName}, possibly augments with
 * new mentions, prunes some high-noise mentions, and writes out
 * {@link PropertyKeys#cleanedMentionsFileName} to be loaded by
 * {@link MentionTrie} to be used on the payload corpus.
 * 
 * @author soumen
 */
public class MentionFilter {
	/**
	 * @param args [0]=conf, [1]=log, [2..]=filters
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		MentionFilter mtf = new MentionFilter(conf);
		mtf.filterMain(new ObjectOpenHashSet<String>(Arrays.copyOfRange(args, 2, args.length)));
		mtf.transcribeReferenceMentionWithCounts();
		mtf.close();
	}

	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final ACatalog catalog;
	final HashSet<String> discardedPhrases;
	
	@SuppressWarnings("unchecked")
	MentionFilter(Config conf) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		this.conf = conf;
		catalog = ACatalog.construct(conf);
		final File discardedPhrasesFile = new File(conf.getString(PropertyKeys.discardedMentionsFileKey));
		if (discardedPhrasesFile.canRead()) {
			discardedPhrases = (HashSet<String>) BinIO.loadObject(discardedPhrasesFile);
			logger.info("Loaded " + discardedPhrases.size() + " discarded phrases from " + discardedPhrasesFile);
		}
		else {
			discardedPhrases = new HashSet<String>();
		}
	}
	
	void close() { }
	
	/**
	 * Given we deal with only a few million mentions we will load the raw mentions
	 * file into RAM, discard some, and write the rest to the cleaned mentions file.
	 * @param filterNames
	 * @throws IOException 
	 */
	void filterMain(Set<String> filterNames) throws Exception {
		final MemoryStatus memoryStatus = new MemoryStatus();
		final File mergedMentionFile = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.mergedMentionsFileName));
		final ReferenceArrayList<MentionRecord> mentions = new ReferenceArrayList<MentionRecord>();
		final DataInputStream mentionsDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(mergedMentionFile)));
		try {
			for (;;) {
				final MentionRecord mention = new MentionRecord();
				mention.load(mentionsDis);
				mentions.add(mention);
			}
		}
		catch (EOFException eofx) {
			logger.info("Read " + mentions.size() + " input mentions, " + memoryStatus + ".");
		}
		mentionsDis.close();
		
		if (filterNames.contains("AnnotationProbabilityThreshold")) {
			filterByAnnotationProbabilityThreshold(mentions);
		}
		
		if (filterNames.contains("TokenMismatch")) {
			filterByTokenMismatch(mentions);
		}

		int nSaved = 0;
		final File cleanedMentionFile = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.cleanedMentionsFileName));
		final DataOutputStream mentionsDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(cleanedMentionFile)));
		for (MentionRecord mention : mentions) {
			mention.store(mentionsDos);
			++nSaved;
		}
		mentionsDos.close();
		logger.info("Saved " + nSaved + " mentions.");
		final File discardedPhrasesFile = new File(conf.getString(PropertyKeys.discardedMentionsFileKey));
		BinIO.storeObject(discardedPhrases, discardedPhrasesFile);
		logger.info("Wrote " + discardedPhrases.size() + " discarded phrases to " + discardedPhrasesFile);
	}
	
	/**
	 * For diagnostic printing.
	 * @throws IOException
	 */
	void transcribeReferenceMentionWithCounts() throws IOException {
    	final File cleanedFile = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.cleanedMentionsFileName));
    	final MentionTrie trie = new MentionTrie(catalog, cleanedFile, Level.DEBUG);
    	trie.getClass();
    	// as if the above is not enough...
    	final DataInputStream cleanedDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(cleanedFile)));
    	try {
    		MentionRecord em = new MentionRecord();
    		for (;;) {
    			em.load(cleanedDis);
    			logger.trace(em);
    		}
    	}
    	catch (EOFException eofx) { }
    	cleanedDis.close();
	}
	
	void collectMarginalMentionCounts(List<MentionRecord> mentions, TObjectIntHashMap<String> mentionCounts) throws IOException {
		mentionCounts.clear();
		final PhraseWriter phraseWriter = new PhraseWriter();
		for (MentionRecord mention : mentions) {
			mentionCounts.adjustOrPutValue(phraseWriter.makePhrase(mention), mention.count, mention.count);
    	}
		logger.info("Read " + mentions.size() + " mentions, " + mentionCounts.size() + " distinct phrases");
	}
	
	/**
	 * Currently this does not do any real filtering but just prints
	 * mention phrases in decreasing order of the fraction of times the mention
	 * phrase is annotated in the reference corpus.
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	@SuppressWarnings("unchecked")
	private void filterByAnnotationProbabilityThreshold(ReferenceArrayList<MentionRecord> mentions) throws IOException, ClassNotFoundException {
		final double minAnnotProb = conf.getDouble(iitb.CSAW.Spotter.PropertyKeys.minAnnotProbName);
		logger.info("Filtering by annotation probability threshold=" + minAnnotProb);
		logger.info("Starting with " + mentions.size() + " mentions");
		final PhraseWriter pw = new PhraseWriter();
    	final File phraseCountsFile = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.phraseCountsFileName));
    	final TObjectIntHashMap<String> phraseCorpusCounts = (TObjectIntHashMap<String>) BinIO.loadObject(phraseCountsFile);
    	phraseCorpusCounts.forEachEntry(new TObjectIntProcedure<String>() {
			@Override
			public boolean execute(String phrase, int count) {
				if (phrase == null || count == 0) {
					throw new IllegalStateException(); 
				}
				return true;
			}
		});
    	
    	final TObjectIntHashMap<String> phraseAnnotCounts = new TObjectIntHashMap<String>();
    	collectMarginalMentionCounts(mentions, phraseAnnotCounts);
    	phraseAnnotCounts.forEachEntry(new TObjectIntProcedure<String>() {
			@Override
			public boolean execute(String phrase, int count) {
				if (phrase == null || count == 0) {
					throw new IllegalStateException(); 
				}
				return true;
			}
		});
    	
    	logger.info("corpus=" + phraseCorpusCounts.size() + " annot=" + phraseAnnotCounts.size());
    	final MutableInt nMissing = new MutableInt(0);
    	phraseAnnotCounts.forEachKey(new TObjectProcedure<String>() {
			@Override
			public boolean execute(String phrase) {
				if (!phraseCorpusCounts.containsKey(phrase)) {
					logger.debug("Corpus count zero for " + phrase + " " + phraseAnnotCounts.get(phrase));
					nMissing.increment();
				}
				return true;
			}
		});
    	logger.warn(nMissing + " phrases missing");
    	
    	printByIncreasingAnnotationProbability(phraseCorpusCounts, phraseAnnotCounts);
    	
    	// also remove mention phrases with low annotation probability
    	ProgressLogger pl = new ProgressLogger(logger);
    	pl.expectedUpdates = mentions.size();
    	pl.start();
    	int nRemoved = 0;
    	for (Iterator<MentionRecord> mx = mentions.iterator(); mx.hasNext(); ) {
    		pl.update();
    		final MentionRecord mention = mx.next();
    		final String phrase = pw.makePhrase(mention);
    		final double corpusCount = phraseCorpusCounts.get(phrase);
    		final double annotCount = phraseAnnotCounts.get(phrase);
    		if (annotCount / corpusCount < minAnnotProb) {
    			logger.debug("REMOVE filterByAnnotationProbabilityThreshold " + mention);
    			discardedPhrases.add(phrase);
    			mx.remove();
    			++nRemoved;
    		}
    	}
    	pl.stop();
    	pl.done();
    	logger.info("Removed " + nRemoved + " mentions");
    	logger.info("Finishing with " + mentions.size() + " mentions");
	}
	
	/**
	 * Can be used to set the annotation probability threshold
	 * @param phraseCorpusCounts
	 * @param phraseAnnotCounts
	 * @throws FileNotFoundException 
	 */
	private void printByIncreasingAnnotationProbability(final TObjectIntHashMap<String> phraseCorpusCounts, final TObjectIntHashMap<String> phraseAnnotCounts) throws FileNotFoundException {
    	final ReferenceArrayList<String> phraseSorter = new ReferenceArrayList<String>();
    	phraseCorpusCounts.forEachKey(new TObjectProcedure<String>() {
			@Override
			public boolean execute(String object) {
				phraseSorter.add(object);
				return true;
			}
		});
    	for (String phrase : phraseSorter) {
    		if (phrase == null) {
    			throw new IllegalStateException();
    		}
    	}
    	
    	Sorting.quickSort(phraseSorter.elements(), 0, phraseSorter.size(), new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				final double corpusCount1 = phraseCorpusCounts.get(o1);
				final double annotCount1 = phraseAnnotCounts.get(o1);
				final double corpusCount2 = phraseCorpusCounts.get(o2);
				final double annotCount2 = phraseAnnotCounts.get(o2);
				return (int) Math.signum(annotCount1/corpusCount1 - annotCount2/corpusCount2);
			}
		});
    	
    	final File sysTmpDir = new File(System.getProperty("java.io.tmpdir"));
    	final File sortedPhraseFile = new File(sysTmpDir, "SortedPhrases.txt");
    	final PrintStream sortedPhraseStream = new PrintStream(sortedPhraseFile);
    	for (String phrase : phraseSorter) {
    		final double cc = phraseCorpusCounts.get(phrase), ac = phraseAnnotCounts.get(phrase), frac = ac/cc;
    		sortedPhraseStream.println(cc + " " + ac + " " + frac + " " + phrase);
    	}
    	sortedPhraseStream.close();
	}

	/**
	 * If there is no {@link TermProcessor}'d token match between the standard
	 * lemma of an entity and a mention, the mention is removed.  Not designed
	 * to be efficient because this is a one-time run before the payload corpus
	 * is scanned.
	 * @param mentions
	 * @throws IOException 
	 */
	void filterByTokenMismatch(ReferenceArrayList<MentionRecord> mentions) throws IOException {
		final PhraseWriter pw = new PhraseWriter();
		logger.info("Token mismatch filter beginning with " + mentions.size() + " mentions");
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = mentions.size();
		pl.start("Filtering by annotation token mismatch");
    	int nRemoved = 0;
    	for (Iterator<MentionRecord> mx = mentions.iterator(); mx.hasNext(); ) {
    		final MentionRecord mention = mx.next();
    		pl.update();
			final int entId = catalog.entNameToEntID(mention.entName.toString());
			if (entId < 0) {
    			++nRemoved;
    			mx.remove();
    			logger.debug("REMOVE unknownEntName " + mention);
    			final String phrase = pw.makePhrase(mention);
    			discardedPhrases.add(phrase);
				continue;
			}
			boolean hasOverlap = false;
			if (catalog.entIDToNumLemmas(entId) > 0) {
				MutableString lemma = new MutableString(), word = new MutableString(), nonWord = new MutableString();
				lemmaLoop:
					for (int elx = 0, eln = catalog.entIDToNumLemmas(entId); elx < eln; ++elx) {
						catalog.entIDToLemma(entId, elx, lemma);
						FastBufferedReader lemmaReader = new FastBufferedReader(lemma);
						while (lemmaReader.next(word, nonWord)) {
							for (int mtx = 0, mtn = mention.size(); mtx < mtn; ++mtx) {
								if (mention.token(mtx).equals(word)) {
									hasOverlap = true;
									break lemmaLoop;
								}
							}
						}
						lemmaReader.close();
					}
			}
			else {
				// ent to lemmas not supported, use canonical ent name only
				FastBufferedReader lemmaReader = new FastBufferedReader(catalog.entIDToEntName(entId).toCharArray());
				MutableString word = new MutableString(), nonWord = new MutableString();
				while (lemmaReader.next(word, nonWord)) {
					for (int mtx = 0, mtn = mention.size(); mtx < mtn; ++mtx) {
						if (mention.token(mtx).equals(word)) {
							hasOverlap = true;
							break;
						}
					}
				}
				lemmaReader.close();
			}
			
			if (!hasOverlap) {
    			++nRemoved;
    			mx.remove();
    			logger.debug("REMOVE filterByTokenMismatch " + mention);
    			final String phrase = pw.makePhrase(mention);
    			discardedPhrases.add(phrase);
			}
		}
    	pl.stop();
    	pl.done();
    	logger.info("Removed " + nRemoved + " mentions");		
		logger.info("Finishing with " + mentions.size() + " mentions");
	}
}
