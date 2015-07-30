package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.MentionRecord;
import iitb.CSAW.Spotter.PropertyKeys;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.Sort.ExternalMergeSort;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class MentionCollector {
	/**
	 * @param args [0]=config [1]=log [2..]=op {lemma, mention, merge, count, filter) 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		MentionCollector mc = new MentionCollector(config);
		ReferenceArrayList<File> runFiles = new ReferenceArrayList<File>();
		mc.collectGroundMentions(runFiles);
		mc.mergeCountGroundMentions(runFiles);
		for (File runFile : runFiles) {
			runFile.delete();
		}
//		mc.countGroundPhrasesInReferenceCorpus();
	}

	protected static final int PHRASE_COUNT_HIGH_WATER = 50000;

	final Config config;
	final AStripeManager stripeManager;
	final Logger logger;
	final ProgressLogger pl;
	final ACatalog catalog;
	final TermProcessor termProcessor;
	final TokenCountsReader tcr;
	final File mentionsFile, tmpDir;
	final HashSet<String> discardedPhrases = new HashSet<String>();

	MentionCollector(Config config) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, EnvironmentLockedException, IOException, DatabaseException, InstantiationException, ConfigurationException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		this.catalog = ACatalog.construct(config);
		this.logger = Logger.getLogger(getClass());
		this.pl = new ProgressLogger(logger);
		this.termProcessor = DefaultTermProcessor.construct(config); 
		this.mentionsFile = new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.mentionsFileName));
		this.tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		tcr = new TokenCountsReader(new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
	}
	
	DataOutputStream getDataOutputStream(File file) throws IOException {
		return new DataOutputStream(new FastBufferedOutputStream(/*new GZIPOutputStream*/(new FileOutputStream(file))));
	}
	
	void catchMatch(PhraseWriter pw, MentionRecord mr) {
		final String phrase = pw.makePhrase(mr);
		final String[] clist = new String[] { "talk|page", "edit|help" };
		for (String citem : clist) {
			if (phrase.equals(citem)) {
				logger.warn("YES " + citem);
			}
		}
	}

	void collectGroundMentions(final List<File> runFiles) throws Exception {
		final AtomicInteger runFileCounter = new AtomicInteger(0);
		final int nThreads = config.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		final BarcelonaCorpus corpus = new BarcelonaCorpus(config);
		corpus.reset();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start("Started collecting reference mentions.");
		for (int tx = 0; tx < config.getInt(Config.nThreadsKey); ++tx) {
			final IWorker thread = new IWorker() {
				long wNumDone = 0;
				final int MSIZE = 500000, MFULL = MSIZE/2;
				final PhraseWriter pw = new PhraseWriter();
				HashMap<MentionRecord, MentionRecord> mentions = new HashMap<MentionRecord, MentionRecord>(MSIZE);
				ReferenceArrayList<MentionRecord> mentionSorter = new ReferenceArrayList<MentionRecord>(MSIZE);
				final TermProcessor tp = termProcessor.copy();
				final BarcelonaDocument doc = new BarcelonaDocument();
				final HashSet<String> workerDiscardedPhrases = new HashSet<String>();

				@Override
				public Exception call() throws Exception {
					try {
						while (corpus.nextDocument(doc)) {
							final Iterable<Annotation> referenceAnnots = doc.getReferenceAnnotations();
							// annots include only the span but not the text in it so
							// we have to collect that by scanning the doc
							ReferenceArrayList<String> tokens = new ReferenceArrayList<String>();
							DocumentSpotter.processAllTerms(tp, doc, tokens);
							for (Annotation annot : referenceAnnots) {
								List<String> phrase = tokens.subList(annot.interval.left, annot.interval.right + 1);
								writeOneMention(annot.entName, phrase);
							}
							pl.update();
							++wNumDone;
						}
						flush(true);
						synchronized (discardedPhrases) {
							discardedPhrases.addAll(workerDiscardedPhrases);
							workerDiscardedPhrases.clear();
						}
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
				
				void writeOneMention(String canon, List<String> phrase) throws IOException {
					final int ent = catalog.entNameToEntID(canon);
					if (ent < 0) {
						workerDiscardedPhrases.add(pw.makePhrase(phrase));
						/*
						 * Note that a phrase discarded because one canon was
						 * not found may be included because of another canon
						 * that was found in the catalog.
						 */
						return;
					}
					MentionRecord em = new MentionRecord();
					em.entName.replace(canon);
					em.count = 1;
					for (String word : phrase) {
						em.append(word);
					}
					if (em.size() > 0) {
						if (mentions.containsKey(em)) {
							++mentions.get(em).count;
						}
						else {
							mentions.put(em, em);
						}
					}
					flush(false);
				}

				void flush(boolean force) throws IOException {
					if (force || mentions.size() > MFULL) {
						final String runName = MentionRecord.class.getCanonicalName() + "_" + runFileCounter.getAndIncrement() + ".dat";
						final File runFile = new File(tmpDir, runName);
						runFiles.add(runFile);
						mentionSorter.clear();
						mentionSorter.addAll(mentions.values());
						Collections.sort(mentionSorter, new MentionRecord.MentionEntityComparator());
						logger.info("Flushing sorted run to " + runFile);
						final DataOutputStream runDos = getDataOutputStream(runFile);
						for (MentionRecord mr : mentionSorter) {
							mr.store(runDos);
//							catchMatch(pw, mr);
						}
						runDos.close();
						mentions.clear();
						mentions = new HashMap<MentionRecord, MentionRecord>(MSIZE);
					}
				}
				
				@Override
				public long numDone() {
					return wNumDone;
				}
			};
			workerPool.add(thread);
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		pl.stop("Finished collecting reference mentions.");
		pl.done();
		corpus.close();
		final File discardedPhrasesFile = new File(config.getString(PropertyKeys.discardedMentionsFileKey));
		BinIO.storeObject(discardedPhrases, discardedPhrasesFile);
		logger.info("Wrote " + discardedPhrases.size() + " discarded phrases to " + discardedPhrasesFile);
	}

	void mergeCountGroundMentions(List<File> runFiles) throws IOException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		final File concatFile = new File(tmpDir, MentionRecord.class.getCanonicalName() + ".dat");
    	MentionRecord.MentionEntityComparator mec = new MentionRecord.MentionEntityComparator();
		ExternalMergeSort<MentionRecord> mentionSorter = new ExternalMergeSort<MentionRecord>(MentionRecord.class, mec, false, tmpDir);
		mentionSorter.mergeFanIn(runFiles, concatFile);
		
    	// scan merged file and accumulate counts into output file
    	final File mergedFile = new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.mergedMentionsFileName));
		logger.info("Collecting counts from " + concatFile + " into " + mergedFile);
    	final DataInputStream sortedDis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(concatFile)));
    	final DataOutputStream mergedDos = new DataOutputStream(new FastBufferedOutputStream(new FileOutputStream(mergedFile)));
    	MentionRecord lastEm = new MentionRecord(), curEm = new MentionRecord();
    	int nEmRead = 0, nEmWritten = 0, runCount = 0;
//    	final PhraseWriter pw = new PhraseWriter();
    	
		for (;;) {
			try {
				curEm.load(sortedDis);
//				catchMatch(pw, curEm);
	    	}
			catch (EOFException eofx) {
				break;
			}
			if (lastEm.entName.length() > 0) {
				final int compare = mec.compare(lastEm, curEm); 
				if (compare > 0) {
					throw new IllegalStateException("Entity mention input records not sorted, prev=" + lastEm + " this=" + curEm);
				}
				if (compare == 0) {
				}
				if (compare < 0) {
					lastEm.count = runCount;
					lastEm.store(mergedDos);
					runCount = 0;
					++nEmWritten;
				}
			}
			++nEmRead;
			runCount += curEm.count;
			lastEm.replace(curEm);
		}
    	if (lastEm.entName.length() > 0) {
    		lastEm.count = runCount;
    		lastEm.store(mergedDos);
    		++nEmWritten;
    	}
		logger.info("Read " + nEmRead + " wrote " + nEmWritten + " records");
    	sortedDis.close();
    	mergedDos.close();
    	concatFile.delete();
	}
	
	/**
	 * Collect corpus freq of all mention phrases, annotated or not.
	 * Note that this cannot be done by scanning the merged annotations!
	 * @throws Exception
	 */
	void countGroundPhrasesInReferenceCorpus() throws Exception {
		final int nThreads = config.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
    	final TObjectIntHashMap<String> sharedPhraseCounts = new TObjectIntHashMap<String>(8000000);
		final BarcelonaCorpus corpus = new BarcelonaCorpus(config);
		corpus.reset();
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.logInterval = ProgressLogger.ONE_MINUTE/2;
		pl.start("Started collecting phrase counts.");
		for (int tx = 0; tx < config.getInt(Config.nThreadsKey); ++tx) {
			final IWorker thread = new IWorker() {
				final TermProcessor localTermProcessor = termProcessor.copy();
		    	final DocumentSpotter spotter = new DocumentSpotter(config, tcr);
				final BarcelonaDocument doc = new BarcelonaDocument();
				final ReferenceArrayList<String> processedTokens = new ReferenceArrayList<String>();
		    	final TObjectIntHashMap<String> localPhraseStringCounts = new TObjectIntHashMap<String>(PHRASE_COUNT_HIGH_WATER);
		    	final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
				final PhraseWriter phraseMaker = new PhraseWriter(); 
				long wNumDone = 0;
				
				@Override
				public Exception call() throws Exception {
					try {
						while (corpus.nextDocument(doc)) {
							++wNumDone;
							pl.update();
							DocumentSpotter.processAllTerms(localTermProcessor, doc, processedTokens);
							spotter.scanMaximal(processedTokens, spots);
							for (Spot spot : spots) {
								final String phrase = phraseMaker.makePhrase(processedTokens.subList(spot.span.left, spot.span.right+1));
								localPhraseStringCounts.adjustOrPutValue(phrase, 1, 1);
							}
							// accumulate per thread counts into shared counts
							if (localPhraseStringCounts.size() > PHRASE_COUNT_HIGH_WATER) {
								transfer();
							}
						}
						transfer();
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
				
				void transfer() {
					synchronized (sharedPhraseCounts) {
						localPhraseStringCounts.forEachEntry(new TObjectIntProcedure<String>() {
							@Override
							public boolean execute(String phrase, int count) {
								sharedPhraseCounts.adjustOrPutValue(phrase, count, count);
								return true;
							}
						});
					}
					localPhraseStringCounts.clear();
				}

				@Override
				public long numDone() {
					return wNumDone;
				}
			};
			workerPool.add(thread);
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		pl.stop("Finished collecting phrase counts.");
		pl.done();
    	final File phraseCountsFile = new File(config.getString(iitb.CSAW.Spotter.PropertyKeys.phraseCountsFileName));
    	BinIO.storeObject(sharedPhraseCounts, phraseCountsFile);
    	corpus.close();
	}
}
