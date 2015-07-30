package iitb.CSAW.Index.SIP1;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Index.PropertyKeys;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.util.Interval;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import cern.colt.Sorting;

import com.jamonapi.MonitorFactory;

/**
 * Intermediate stages use a non-compact representation of a posting list as a
 * sequence of {@link int}s (usually 32 bits). Merges happen in this format.
 * Finally a compressed version is written out.  The compact doc posting 
 * format is:
 * <ul>
 * <li>catId, {@link int} in non-compact, removed in compact</li>
 * <li>docId, {@link int}</li>
 * <li>dictSize, gamma code</li>
 * <li>sequence of "long" entIds in dictionary, each an {@link int}</li>
 * <li>nPosts, gamma code</li>
 * <li>each post has a pgap (gamma) and a short ent ID (gamma)</li>
 * </ul>
 * Note that a <em>long</em> ent ID is still an {@link int}, not a {@link long}.
 * A <em>short</em> ent ID is an offset in the entity dictionary associated
 * with a specific type and a document block within that type's posting list.
 * @author soumen
 */
public class Sip1IndexWriter {
	// cosmic constants
	static final int MAX_TYPE_KEYS = 100, MAX_DOC_BUF_INTS = 10000, MAX_BATCH_BUF_INTS = 100000000;

	// setup
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final AStripeManager stripeManager;
	final String fieldName;
	final ACatalog catalog;
	
	// batch items
	final AtomicInteger sharedBatchCounter;
	long batchBeginTime = System.currentTimeMillis(), nDocsInBatch = 0;
	final IntArrayList batchIntBuffer = new IntArrayList(MAX_BATCH_BUF_INTS);
	final IntArrayList batchSorter = new IntArrayList(MAX_DOC_BUF_INTS);

	// below members reused from document to document
	final TIntHashSet catIds = new TIntHashSet(MAX_TYPE_KEYS);
	final IntArrayList catScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList entScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList catEntSort = new IntArrayList(MAX_DOC_BUF_INTS);
	final TIntIntHashMap entToFreqMap = new TIntIntHashMap(MAX_TYPE_KEYS);
	final IntArrayList entFreqSort = new IntArrayList(MAX_DOC_BUF_INTS);
	final TIntIntHashMap catToPutOffset = new TIntIntHashMap(MAX_DOC_BUF_INTS);
	
	public Sip1IndexWriter(Config config, String fieldName, AtomicInteger batchCounter) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		this.fieldName = fieldName;
		this.catalog = ACatalog.construct(config);
		this.sharedBatchCounter = batchCounter;
	}
	
	private void reuse() {
		catIds.clear();
		catScratch.clear();
		entScratch.clear();
		catEntSort.clear();
		entToFreqMap.clear();
		entFreqSort.clear();
		catToPutOffset.clear();
	}
	
	void decomposeSpans(ReferenceArrayList<Annotation> inAnnots, ReferenceArrayList<Annotation> outAnnots) {
		outAnnots.clear();
		for (Annotation inAnno : inAnnots) {
			for (int pos : inAnno.interval) {
				final Annotation outAnno = new Annotation(inAnno.entName, Interval.valueOf(pos), inAnno.score, inAnno.rank);
				outAnnots.add(outAnno);
			}
		}
		Collections.sort(outAnnots);
	}

	/**
	 * For now we will fake an interval by injecting each contained token position.
	 * @param docId
	 * @param annots
	 * @throws IOException 
	 */
	public void indexOneDocument(int docId, ReferenceArrayList<Annotation> spanAnnots) throws IOException {
		final ReferenceArrayList<Annotation> decomposedAnnots = new ReferenceArrayList<Annotation>();
		decomposeSpans(spanAnnots, decomposedAnnots);
		reuse();
		++nDocsInBatch;
		// first scan
		for (Annotation anno : decomposedAnnots) {
			final int entId = catalog.entNameToEntID(anno.entName);
			if (entId < 0) {
				throw new IllegalArgumentException(anno.entName + " not mapped to ID");
			}
			catIds.clear();
			catalog.catsReachableFromEnt(entId, catIds);
			catIds.forEach(new TIntProcedure() {
				@Override
				public boolean execute(int catId) {
					catScratch.add(catId);
					entScratch.add(entId);
					return true;
				}
			});
		}
		// sort scratch
		assert catScratch.size() == entScratch.size();
		for (int sx = 0, nx = catScratch.size(); sx < nx; ++sx) {
			catEntSort.add(sx);
		}
		Collections.sort(catEntSort, new IntComparator() {
			@Override
			public int compare(int k1, int k2) {
				final int c1 = catScratch.getInt(k1) - catScratch.getInt(k2);
				if (c1 != 0) return c1;
				final int c2 = entScratch.getInt(k1) - entScratch.getInt(k2);
				return c2;
			}
			@Override
			public int compare(Integer o1, Integer o2) {
				return compare(o1.intValue(), o2.intValue());
			}
		});
		// write the dictionaries
		int runCatId = -1;
		for (int sx = 0, nx = catEntSort.size(); sx < nx; ++sx) {
			final int pointer = catEntSort.getInt(sx);
			final int catId = catScratch.getInt(pointer);
			final int entId = entScratch.getInt(pointer);
			if (runCatId != catId && runCatId != -1) {
				assert catId > runCatId;
				writePreludeToPostingList(runCatId, docId);
				entToFreqMap.clear();
			}
			runCatId = catId;
			entToFreqMap.adjustOrPutValue(entId, 1, 1);
		}
		if (runCatId != -1) {
			writePreludeToPostingList(runCatId, docId);
		}
		// second scan
		for (Annotation anno : decomposedAnnots) {
			assert anno.interval.left == anno.interval.right;
			final int pos = anno.interval.left;
			final int entId = catalog.entNameToEntID(anno.entName);
			if (entId < 0) {
				throw new IllegalArgumentException(anno.entName + " not mapped to ID");
			}
			catIds.clear();
			catalog.catsReachableFromEnt(entId, catIds);
			catIds.forEach(new TIntProcedure() {
				@Override
				public boolean execute(int catId) {
					// locate offset where to write post
					assert catToPutOffset.containsKey(catId);
					final int postBase = catToPutOffset.get(catId);
					// write absolute position (to be converted to pgap before flush)
					batchIntBuffer.set(postBase, pos);
					// write long entity ID (to be converted to short ID before flush)
					batchIntBuffer.set(postBase + 1, entId);
					// bump cursor by 2 int32s
					catToPutOffset.adjustValue(catId, 2);
					return true;
				}
			});
		}
		if (isCrowded()) {
			flushBatch();
		}
	}

	private void writePreludeToPostingList(final int catId, final int docId) {
		entFreqSort.clear();
		// sum of entity frequencies is the number of postings to follow the dictionary
		final MutableInt nPostsToFollow = new MutableInt(0);
		entToFreqMap.forEachEntry(new TIntIntProcedure() {
			@Override
			public boolean execute(int entId, int freq) {
				entFreqSort.add(entId);
				nPostsToFollow.add(freq);
				return true;
			}
		});
		Collections.sort(entFreqSort, new IntComparator() {			
			@Override
			public int compare(Integer o1, Integer o2) {
				return compare(o1.intValue(), o2.intValue());
			}
			@Override
			public int compare(int k1, int k2) { // decreasing order
				return entToFreqMap.get(k2) - entToFreqMap.get(k1);
			}
		});
		// calculate number of ints needed for this catId and this document
		final int dictSize = entFreqSort.size();
		int nIntsToAlloc = 0;
		nIntsToAlloc += 3; // catId, docId, dictSize
		nIntsToAlloc += dictSize; // dict
		nIntsToAlloc += 1; // nPosts
		nIntsToAlloc += 2 * nPostsToFollow.intValue(); // posts (pos, ent)
		// allocate space and cursor
		int cursor = batchIntBuffer.size();
		batchIntBuffer.size(cursor + nIntsToAlloc);
		// write prelude 
		batchIntBuffer.set(cursor++, catId);
		batchIntBuffer.set(cursor++, docId);
		batchIntBuffer.set(cursor++, dictSize);
		for (int fx = 0; fx < dictSize; ++fx) {
			// write long ent IDs in decreasing order of frequency
			batchIntBuffer.set(cursor++, entFreqSort.get(fx));
		}
		// write the number of posting entries to follow
		batchIntBuffer.set(cursor++, nPostsToFollow.intValue());
		// initialize offset where posts will be written next
		catToPutOffset.put(catId, cursor);
	}
	
	private boolean isCrowded() {
		return batchIntBuffer.size() > 0.9d * MAX_BATCH_BUF_INTS;
	}
	
	/**
	 * Turn absolute positions into pgaps and long ent IDs into short ent IDs.
	 * Fill an indirection array to point to the first ints of document records
	 * in (catId, docId) order.
	 */
	private void rewriteForFlush() {
		batchSorter.clear();
		for (int cursor = 0, cMax = batchIntBuffer.size(); cursor < cMax; ) {
			batchSorter.add(cursor);
			/* final int catId = */ batchIntBuffer.getInt(cursor++);
			/* final int docId = */ batchIntBuffer.getInt(cursor++);
			final int dictSize = batchIntBuffer.getInt(cursor++);
			MonitorFactory.add("dictSize", "", dictSize);
			final int dictBase = cursor;
			final int dictEnd = dictBase + dictSize;
			final IntList entDict = batchIntBuffer.subList(dictBase, dictEnd);
			cursor += dictSize;
			final int nPosts = batchIntBuffer.getInt(cursor++);
			for (int px = 0, prevPos = 0; px < nPosts; ++px) {
				final int absPos = batchIntBuffer.getInt(cursor);
				assert absPos >= prevPos;
				final int pgap = absPos - prevPos;
				MonitorFactory.add("pgap", "", pgap);
				batchIntBuffer.set(cursor++, pgap);
				prevPos = absPos;
				final int entLongId = batchIntBuffer.getInt(cursor);
				final int entShortId = entDict.indexOf(entLongId);
				assert 0 <= entShortId && entShortId < dictSize;
				batchIntBuffer.set(cursor++, entShortId);
			}
			// ends one doc entry
		}
		Sorting.quickSort(batchSorter.elements(), 0, batchSorter.size(), new cern.colt.function.IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final int cat1 = batchIntBuffer.getInt(o1);
				final int cat2 = batchIntBuffer.getInt(o2);
				final int cmpCat = cat1 - cat2;
				if (cmpCat != 0) return cmpCat;
				final int doc1 = batchIntBuffer.getInt(o1+1);
				final int doc2 = batchIntBuffer.getInt(o2+1);
				return doc1 - doc2;
			}
		});
	}
	
	public void flushBatch() throws IOException {
		reuse();
		rewriteForFlush();
		if (batchIntBuffer.isEmpty()) return;
		final int batchNumber = sharedBatchCounter.getAndIncrement();
		final File indexDir = stripeManager.mySipIndexRunDir();
		final File sipFile = new File(indexDir, fieldName + "_" + batchNumber + PropertyKeys.sipUncompressedPostingsExtension);
		final DataOutputStream sipDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(sipFile)));
		// verify and flush in permuted order
		final Sip1Document idp = new Sip1Document(); 
		for (int prevCatId = -1, prevDocId = -1, bsx = 0, bsMax = batchSorter.size(); bsx < bsMax; ++bsx) {
			final int pointer = batchSorter.getInt(bsx);
			idp.load(batchIntBuffer, pointer);
			final int catId = idp.catId();
			final int docId = idp.docId();
			if (catId < prevCatId || docId <= prevDocId) {
				throw new IllegalStateException("sort(catId,docId) mangled");
			}
			idp.store(sipDos);
		}
		sipDos.close();
		// collect stats
		final long sipFileLength = sipFile.length();
		final double bytesPerInt = 1d * sipFileLength / batchIntBuffer.size();
		final long now = System.currentTimeMillis();
//		final double batchDocsPerMs = 1d * nDocsInBatch / (now - batchBeginTime);
//		final double batchPostsPerMs = 1d * nBatchPosts / (now - batchBeginTime);
		final double avgDictSize = MonitorFactory.getMonitor("dictSize", "").getAvg();
		final double avgPgap = MonitorFactory.getMonitor("pgap", "").getAvg();
		logger.info("Batch #" + batchNumber + " ints=" + batchIntBuffer.size() + " byte/int=" + bytesPerInt + " avgDictSize=" + avgDictSize + " avgPgap=" + avgPgap);
		batchBeginTime = now;
		// clean out the RAM
		batchIntBuffer.clear();
		batchSorter.clear();
		nDocsInBatch = 0;
	}
}
