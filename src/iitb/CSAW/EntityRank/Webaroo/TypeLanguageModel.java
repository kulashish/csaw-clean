package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.THashSet;
import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

/**
 * Rudimentary unigram language model around lemmas and canonical names for
 * categories/types in the {@link ACatalog}, with a diagnostic method to search
 * for types by a small bag of words query.
 *  
 * @author soumen
 * @since 2012/10/01
 */
public class TypeLanguageModel {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		final Config conf = new Config(args[0], args[1]);
		final double smoother = Double.parseDouble(args[2]);
		TypeLanguageModel tlm = new TypeLanguageModel(conf, smoother);
		LineNumberReader lnr = new LineNumberReader(new InputStreamReader(System.in));
		for (;;) {
			System.out.print("Query: ");
			System.out.flush();
			tlm.bagOfWordsTypeSearch(new ReferenceArrayList<String>(lnr.readLine().trim().split("\\s+")), 20);
		}
	}

	final Config conf;
	final File emptyScoreFile;
	final double smoother;
	final ACatalog catalog;
	final Logger logger = Logger.getLogger(getClass());
	final TObjectIntHashMap<String> globalDocFreq = new TObjectIntHashMap<String>();
	final ReferenceArrayList<String[]> catToWords;
	final double[] emptyScores;
	
	public TypeLanguageModel(Config conf, double smoother) throws Exception {
		this.conf = conf;
		catalog = ACatalog.construct(conf);
		catToWords = new ReferenceArrayList<String[]>(catalog.numCats());
		catToWords.size(catalog.numCats());
		this.smoother = smoother;
		buildFastPart();
		AStripeManager asm = AStripeManager.construct(conf);
		File tmpDir = asm.getTmpDir(asm.myHostStripe());
		// use the smoother value as part of the cached file name
		// may cause extra computation if long bits become slightly different
		final long smootherLong = Double.doubleToLongBits(smoother);
		emptyScoreFile = new File(tmpDir, getClass().getCanonicalName() + ".emptyScore_" + Long.toHexString(smootherLong) + ".dat");
		if (emptyScoreFile.canRead()) {
			logger.info("Loading " + emptyScoreFile);
			emptyScores = BinIO.loadDoubles(emptyScoreFile);
		}
		else {
			logger.info("Creating new " + emptyScoreFile);
			emptyScores = new double[catalog.numCats()];
			buildSlowPart();
		}
	}
	
	private void buildFastPart() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException  {
		double sumNumCatWords = 0;
		for (int cat = 0, ncat = catalog.numCats(); cat < ncat; ++cat) {
			THashSet<String> catWords = new THashSet<String>();
			fillCatWords(catWords, catalog.catIDToCatName(cat));
			MutableString oneLemma = new MutableString();
			for (int lx = 0, ln = catalog.catIDToNumLemmas(cat); lx < ln; ++lx) {
				catalog.catIDToLemma(cat, lx, oneLemma);
				fillCatWords(catWords, oneLemma.toString());
			}
			final String[] catWordArray = catWords.toArray(new String[]{});
			Arrays.sort(catWordArray);
			catToWords.set(cat, catWordArray);
			sumNumCatWords += catWords.size();
			for (Iterator<String> cwx = catWords.iterator(); cwx.hasNext(); ) {
				globalDocFreq.adjustOrPutValue(cwx.next(), 1, 1);
			}
		}
	}
	
	private void buildSlowPart() throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException  {
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = catalog.numCats();
		pl.start("Initializing emptyScores");
		for (int cat = 0, ncat = catalog.numCats(); cat < ncat; ++cat) {
			emptyScores[cat] = 0;
			for (TObjectIntIterator<String> gdfx = globalDocFreq.iterator(); gdfx.hasNext(); ) {
				gdfx.advance();
				final double pword = probWordGivenType(gdfx.key(), cat);
				if (0 == pword || pword == 1) throw new IllegalStateException();
				emptyScores[cat] += Math.log(1d - pword);
			}
			pl.update();
		}
		pl.stop();
		pl.done();
		BinIO.storeDoubles(emptyScores, emptyScoreFile);
	}
	
	private void fillCatWords(THashSet<String> catWords, String oneLemma) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		if (oneLemma == null) return;
		oneLemma = catalog.canonicalToFreeText(oneLemma);
		final FastBufferedReader tokenizer;
		final TermProcessor termProcessor;
		tokenizer = new FastBufferedReader();
		tokenizer.setReader(new StringReader(oneLemma));
		termProcessor = DefaultTermProcessor.construct(conf);
		try {
			MutableString word = new MutableString(), nonWord = new MutableString();
			for (;;) {
				if (!tokenizer.next(word, nonWord)) { break; }
				if (!termProcessor.processTerm(word)) { continue; }
				if (!catWords.contains(word)) {
					catWords.add(word.toString().toLowerCase());
				}
			}
		}
		catch (EOFException eofx) { }
	}
	
	/**
	 * @param word already {@link TermProcessor}'d
	 * @param cat
	 * @return unsmoothed probability
	 */
	private double probWordGivenTypeUnsmoothed(String word, int cat) {
		return Arrays.binarySearch(catToWords.get(cat), word) >= 0? 1 : 0;
	}
	
	/**
	 * Using Laplacian smoothing, might not be best.
	 * @param word already {@link TermProcessor}'d
	 * @return smoothed probability
	 */
	private double probWordGivenGlobalModel(String word) {
		return (1d + globalDocFreq.get(word)) / (2d + catalog.numCats());
	}

	/**
	 * @param word already {@link TermProcessor}'d
	 * @param cat
	 * @return probabilities spliced using {@link #smoother} 
	 */
	public double probWordGivenType(String word, int cat) {
		return (1d - smoother) * probWordGivenTypeUnsmoothed(word, cat) + smoother * probWordGivenGlobalModel(word);
	}
	
	public double oddsWordGivenType(String word, int cat) {
		final double prob = probWordGivenType(word, cat);
		assert 0 < prob && prob < 1;
		return prob / (1d - prob);
	}
	
	public double logOddsWordGivenType(String word, int cat) {
		return Math.log(oddsWordGivenType(word, cat));
	}
	
	public double logProbEmptyGivenType(int cat) {
		return emptyScores[cat];
	}
	
	private void bagOfWordsTypeSearch(Collection<String> words, int topk) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		final int[] catArray = new int[catalog.numCats()];
		final double[] scoreArray = new double[catalog.numCats()];
		for (int cx = 0, cn = catalog.numCats(); cx < cn; ++cx) {
			catArray[cx] = cx;
			scoreArray[cx] = logProbEmptyGivenType(cx);
		}
		for (String word : words) {
			MutableString mword = new MutableString(word);
			final TermProcessor termProcessor;
			termProcessor = DefaultTermProcessor.construct(conf);
			if (!termProcessor.processTerm(mword)) {
				logger.warn("Cannot use term " + word);
				continue;
			}
			word = mword.toString();
			for (int cx = 0, cn = catalog.numCats(); cx < cn; ++cx) {
				scoreArray[cx] += logOddsWordGivenType(word, cx);
			}
		}
		Sorting.quickSort(catArray, 0, catArray.length, new IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				return - (int) Math.signum(scoreArray[o1] - scoreArray[o2]);
			}
		});
		for (int kx = 0; kx < topk; ++kx) {
			System.out.println(catalog.catIDToCatName(catArray[kx]) + " " + new ReferenceArrayList<String>(catToWords.get(catArray[kx])) + " " + scoreArray[catArray[kx]]);
		}
	}
}
