package iitb.CSAW.Corpus.Webaroo;

import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TLongLongHashMap;
import gnu.trove.TLongLongIterator;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Spotter.ContextFeatureVector;
import iitb.CSAW.Spotter.ContextRecord;
import iitb.CSAW.Spotter.ContextRecordCompact;
import iitb.CSAW.Spotter.DocumentSpotter;
import iitb.CSAW.Spotter.LeafFeatureCountRecord;
import iitb.CSAW.Spotter.LeafFeatureEntityMaps;
import iitb.CSAW.Spotter.LfemSkipAllocator;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IWorker;
import iitb.CSAW.Utils.LongIntInt;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.Sort.BitExternalMergeSort;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import cc.mallet.util.RegexFileFilter;
import cern.colt.Sorting;
import cern.colt.function.LongComparator;

/**
 * <p>Collects (leaf, feat) workload frequencies to tune skips/sentinels
 * in {@link LeafFeatureEntityMaps}.  Should be run on a single-stripe sample
 * of the workload corpus with a "fake" {@link AStripeManager}.</p>
 * 
 * <p>The output is purposely directed to a temporary directory from
 * where it has to transfered to some shared file system after inspection
 * for use with {@link LfemSkipAllocator}.</p>
 *  
 * @author soumen
 */
public class LeafFeatureWorkload {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log
	 * [2]=fraction between 0 and 0.5
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		final double fraction = Double.parseDouble(args[2]);
		assert 0 < fraction && fraction <= 0.5;
		TokenCountsReader tcr = new TokenCountsReader(new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.tokenCountsDirName)));
		LeafFeatureWorkload lfw = new LeafFeatureWorkload(conf, tcr, fraction);
		lfw.collectLeafFeatWorkload();
		lfw.mergeAndAggregate();
	}

	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final double fraction;
	final AStripeManager stripeManager;
	final TokenCountsReader tcr;
	final int LFCR_CAP = 1 * (1<<20), LFCR_HW = LFCR_CAP * 7/10;
	final AtomicInteger lfcRunCounter = new AtomicInteger(0);
	static final String trainName = "TrainWorkload", testName = "TestWorkload";
	static final String lfcrName = LeafFeatureCountRecord.class.getCanonicalName();
	final File trainDir, testDir;
	
	public LeafFeatureWorkload(Config conf, TokenCountsReader tcr, double fraction) throws Exception {
		this.conf = conf;
		this.fraction = fraction;
		stripeManager = AStripeManager.construct(conf);
		this.tcr = tcr;
		final File tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		trainDir = new File(tmpDir, trainName);
		FileUtils.deleteDirectory(trainDir);
		trainDir.mkdir();
		testDir = new File(tmpDir, testName);
		FileUtils.deleteDirectory(testDir);
		testDir.mkdir();
	}

	void collectLeafFeatWorkload() throws Exception {
		final File baseDir = new File(conf.getString(getClass().getSimpleName() + ".SampleCorpusPath") + File.separator + stripeManager.myDiskStripe());
		final WebarooCorpus corpus = new WebarooCorpus(conf, baseDir, false, false);
		corpus.reset();
		WorkerPool pool = new WorkerPool(this, conf.getInt(Config.nThreadsKey));
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = corpus.numDocuments();
		pl.displayFreeMemory = true;
		pl.start("Started collecting leaf, feat workload.");
		for (int tx = 0; tx < conf.getInt(Config.nThreadsKey); ++tx) {
			final int ftx = tx + 1;
			pool.add(new IWorker() {
				long wNumDone = 0;
				final TermProcessor termProcessor = DefaultTermProcessor.construct(conf);
				final DocumentSpotter annotator = new DocumentSpotter(conf, tcr);
				final IAnnotatedDocument idoc = corpus.allocateReusableDocument();
				final ByteArrayList bal = new ByteArrayList();
				final ReferenceArrayList<Spot> spots = new ReferenceArrayList<Spot>();
				final ReferenceArrayList<String> tokens = new ReferenceArrayList<String>();
				final TLongLongHashMap leafFeatToCountMapTrain = new TLongLongHashMap(LFCR_CAP);
				final LongArrayList leafFeatSorterTrain = new LongArrayList(LFCR_CAP);
				final TLongLongHashMap leafFeatToCountMapTest = new TLongLongHashMap(LFCR_CAP);
				final LongArrayList leafFeatSorterTest= new LongArrayList(LFCR_CAP);
				final Random random = new Random(ftx);
				long nTrain =0, nTest = 0;

				@Override
				public Exception call() throws Exception {
					while (corpus.nextDocument(idoc, bal)) {
						DocumentSpotter.processAllTerms(termProcessor, idoc, tokens);
						annotator.scanMaximal(tokens, spots);
						final double coin = random.nextDouble();
						if (coin < 2 * fraction) {
							collectLeafFeatWorkload(coin < fraction, annotator, idoc, tokens, spots);
						}
						pl.update();
						++wNumDone;
					}
					flushSortedLeafFeatWorkloadRun(trainDir, leafFeatToCountMapTrain, leafFeatSorterTrain);
					flushSortedLeafFeatWorkloadRun(testDir, leafFeatToCountMapTest, leafFeatSorterTest);
					logger.info(this + "sampled " + nTrain + " train, " + nTest + " test");
					return null;
				}
				
				/**
				 * One-doc helper for {@link #collectLeafFeatWorkload()}
				 * @param idoc
				 * @param stems
				 * @param spots
				 * @throws IOException 
				 */
				private void collectLeafFeatWorkload(boolean isTrain, DocumentSpotter annotator, IAnnotatedDocument idoc, ReferenceArrayList<String> stems, ReferenceArrayList<Spot> spots) throws IOException {
					if (isTrain) ++nTrain; else ++nTest;
					final LongIntInt lii = new LongIntInt();
					final TIntIntHashMap salientBag = new TIntIntHashMap();
					final ContextFeatureVector cfv = new ContextFeatureVector();
					annotator.pickSalient(idoc, salientBag);
					final ContextRecord cr = new ContextRecord();
					final ContextRecordCompact crc = new ContextRecordCompact();
					for (Spot spot : spots) {
						annotator.collectContextAroundSpot(idoc, stems, spot, cr);
						annotator.collectCompactContextAroundSpot(idoc.docidAsLong(), stems, spot, Spot.unknownEnt, salientBag, crc);
						ContextFeatureVector.makeCountFeatureVector(crc, tcr, cfv);
						for (TIntFloatIterator cfvx = cfv.iterator(); cfvx.hasNext(); ) {
							cfvx.advance();
							lii.write(spot.trieLeafNodeId, cfvx.key());
							if (isTrain) {
								leafFeatToCountMapTrain.adjustOrPutValue(lii.lv, 1, 1);
								if (leafFeatToCountMapTrain.size() > LFCR_HW) {
									flushSortedLeafFeatWorkloadRun(trainDir, leafFeatToCountMapTrain, leafFeatSorterTrain);
								}
							}
							else {
								leafFeatToCountMapTest.adjustOrPutValue(lii.lv, 1, 1);
								if (leafFeatToCountMapTest.size() > LFCR_HW) {
									flushSortedLeafFeatWorkloadRun(testDir, leafFeatToCountMapTest, leafFeatSorterTest);
								}
							}
						}
					}
				}
				
				void flushSortedLeafFeatWorkloadRun(File dir, TLongLongHashMap map, LongArrayList sorter) throws IOException {
					if (map.isEmpty()) {
						return;
					}
					sorter.clear();
					for (TLongLongIterator lfsx = map.iterator(); lfsx.hasNext(); ) {
						lfsx.advance();
						sorter.add(lfsx.key());
					}
					Sorting.quickSort(sorter.elements(), 0, sorter.size(), new LongComparator() {
						final LongIntInt lii1 = new LongIntInt(), lii2 = new LongIntInt();
						@Override
						public int compare(long o1, long o2) {
							lii1.write(o1);
							lii2.write(o2);
							final int cleaf = lii1.iv1 - lii2.iv1;
							if (cleaf != 0) return cleaf;
							return lii1.iv0 - lii2.iv0;
						}
					});
					final LongIntInt lii = new LongIntInt();
					final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord();
					final File lfsRunFile = new File(dir, lfcrName + "_" + Integer.toString(lfcRunCounter.getAndIncrement()) + ".dat");
					logger.info("Flushing LFCR with " + map.size() + " keys to " + lfsRunFile);
					final OutputBitStream lfcObs = new OutputBitStream(lfsRunFile, 8*(1<<20));
					for (long leafFeat : sorter) {
						lii.write(leafFeat);
						lfcr.leaf = lii.iv1;
						lfcr.feat = lii.iv0;
						lfcr.count = map.get(leafFeat);
						lfcr.store(lfcObs);
					}
					lfcObs.close();
					map.clear();
				}

				@Override
				public long numDone() {
					return wNumDone;
				}
			});
		}
		pool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		pl.stop("Finished collecting leaf, feat workload.");
		pl.done();
	}
	
	private void mergeAndAggregate() throws InstantiationException, IllegalAccessException, IOException {
		merge(trainDir);
		aggregate(trainDir);
		merge(testDir);
		aggregate(testDir);
	}

	void merge(File dir) throws InstantiationException, IllegalAccessException, IOException {
		final ArrayList<File> inputFiles = new ArrayList<File>();
		for (File workloadFile : dir.listFiles(new RegexFileFilter(lfcrName + "_\\d+" + ".dat"))) {
			inputFiles.add(workloadFile);
		}
		BitExternalMergeSort<LeafFeatureCountRecord> bems = new BitExternalMergeSort<LeafFeatureCountRecord>(LeafFeatureCountRecord.class, dir);
		final File outputFile = new File(dir, lfcrName + "_merged.dat");
		bems.mergeUsingHeapLimitedFanIn(inputFiles, outputFile);
		for (File inputFile : inputFiles) {
			if (!inputFile.delete()) {
				logger.warn("Cannot delete " + inputFile);
			}
		}
	}
	
	void aggregate(File dir) throws IOException {
		final File sortedLfcrFile = new File(dir, lfcrName + "_merged.dat");
		final File aggrLfcrFile = new File(dir, lfcrName + ".dat");
		logger.info("Aggregating " + sortedLfcrFile + " to " + aggrLfcrFile);
		final InputBitStream sortedLfcrIbs = new InputBitStream(sortedLfcrFile);
		final OutputBitStream aggrLfcrObs = new OutputBitStream(aggrLfcrFile);
		final LeafFeatureCountRecord lfcr = new LeafFeatureCountRecord(), prevLfcr = new LeafFeatureCountRecord();
		final Comparator<LeafFeatureCountRecord> cmp = lfcr.getComparator();
		lfcr.setNull();
		prevLfcr.setNull();
		long inRec = 0, outRec = 0;
		for (;;) {
			try {
				lfcr.load(sortedLfcrIbs);
				++inRec;
				if (prevLfcr.isNull()) {
					prevLfcr.replace(lfcr);
				}
				else {
					if (cmp.compare(prevLfcr, lfcr) > 0) {
						throw new IllegalStateException();
					}
					if (prevLfcr.keyEquals(lfcr)) {
						prevLfcr.count += lfcr.count;
					}
					else {
						prevLfcr.store(aggrLfcrObs);
						++outRec;
						prevLfcr.replace(lfcr);
					}
				}
			}
			catch (EOFException eofx) {
				break;
			}
		}
		if (!prevLfcr.isNull()) {
			prevLfcr.store(aggrLfcrObs);
			++outRec;
		}
		sortedLfcrIbs.close();
		aggrLfcrObs.close();
		logger.info("Aggregated " + inRec + " to " + outRec + " records");
		if (!sortedLfcrFile.delete()) {
			logger.warn("Cannot delete " + sortedLfcrFile);
		}
	}
}
