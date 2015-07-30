package iitb.CSAW.OtherSystems;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntObjectHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.util.StringMap;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.log4j.Logger;

import cern.colt.Sorting;

import com.jamonapi.MonitorFactory;

/**
 * @author ganesh
 */
public class SSQTEIndexWriter {
	// cosmic constants
	static final int MAX_TYPE_KEYS = 100, MAX_DOC_BUF_INTS = 10000, MAX_BATCH_BUF_INTS = 100000000;

	// setup
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final String fieldName;
	final ACatalog catalog;
	
	// batch items
	final AtomicInteger sharedBatchCounter;
	long batchBeginTime = System.currentTimeMillis(), nDocsInBatch = 0;
	final IntArrayList batchIntBuffer = new IntArrayList(MAX_BATCH_BUF_INTS);
	final IntArrayList tokenIdSorter = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList entIdSorter = new IntArrayList(MAX_DOC_BUF_INTS);
	
	// below members reused from document to document
	final IntArrayList tokScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList entScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList tokEntSort = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList tokPosScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	final IntArrayList entPosScratch = new IntArrayList(MAX_DOC_BUF_INTS);
	
	final TIntObjectHashMap<TIntArrayList> ent2OccurenceMap = new TIntObjectHashMap<TIntArrayList>(MAX_TYPE_KEYS);
	final IntArrayList entOrderList = new IntArrayList(MAX_DOC_BUF_INTS);
	
	StringMap<? extends CharSequence> termMap;
	TermProcessor termProcessor;
	final AtomicIntegerArray entIdCount;
	
	public SSQTEIndexWriter(Config config, ACatalog catalog, String fieldName, AtomicInteger batchCounter, 
			StringMap<? extends CharSequence> termMap, AtomicIntegerArray entIdCount, TermProcessor termProcessor) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		this.config = config;
		this.fieldName = fieldName;
		this.catalog = catalog;
		this.sharedBatchCounter = batchCounter;
		this.termMap = termMap;
		this.termProcessor = termProcessor;
		this.entIdCount = entIdCount;
	}
	
	private void reuse() {
		tokScratch.clear();
		entScratch.clear();
		tokEntSort.clear();
		tokPosScratch.clear();
		entPosScratch.clear();
	}

	public void indexOneDocument(IAnnotatedDocument doc) throws IOException {
		// collect tokens and positions
		ReferenceArrayList<Integer> tokens = new ReferenceArrayList<Integer>();
		ReferenceArrayList<Integer> tokenPositions = new ReferenceArrayList<Integer>();
		reuse();
		final MutableString tokenText = new MutableString();
		for (int tokenPos = 0; tokenPos < doc.numWordTokens(); ++tokenPos) {
			tokenText.replace(doc.wordTokenAt(tokenPos));
			if (!termProcessor.processTerm(tokenText)) continue;
			//Since only 6486924 terms in Wikipedia, I am assuming that the term ids will fit in int
			//System.err.println(tokenText);
			tokens.add(termMap.get(tokenText).intValue());
			tokenPositions.add(tokenPos);
			//annots.add(new Annotation(entName.toString(), Interval.valueOf(entPos.intValue())));
		}
		// for each ent annot find nearby tokens
		for (Annotation annot : doc.getReferenceAnnotations()) {
			final int entId = catalog.entNameToEntID(annot.entName);
			if (entId < 0) {
				throw new IllegalArgumentException(annot.entName + " not mapped to ID");
			}
			final int entNewPos = entIdCount.getAndIncrement(entId);
			// look among all tokens for brute-force simplicity
			for (int i = 0; i < tokenPositions.size(); ++i) {
				final int tokPos = tokenPositions.get(i);
				final int tokId = tokens.get(i);
				if (tokPos >= annot.interval.left - iitb.CSAW.OtherSystems.PropertyKeys.SSQ_WINDOW_SIZE && tokPos <= annot.interval.right + iitb.CSAW.OtherSystems.PropertyKeys.SSQ_WINDOW_SIZE) {
					tokScratch.add(tokId);
					tokPosScratch.add(tokPos); 
					entScratch.add(entId); 
					//This will encapsulate both the docId and the position
					entPosScratch.add(entNewPos);
				}
			}
		}
		
		for (int id = 0; id < tokScratch.size(); id++) {
			tokEntSort.add(id);
		}
		++nDocsInBatch;
		Collections.sort(tokEntSort, new IntComparator() {
			@Override
			public int compare(int k1, int k2) {
				final long c1 = tokScratch.get(k1) - tokScratch.get(k2);
				if (c1 < 0) return -1;
				if (c1 > 0) return 1;
				final int c2 = entScratch.getInt(k1) - entScratch.getInt(k2);
				if(c2 != 0) return c2;
				final int c3 = tokPosScratch.getInt(k1) - tokPosScratch.getInt(k2);
				return c3;
			}
			@Override
			public int compare(Integer o1, Integer o2) {
				return compare(o1.intValue(), o2.intValue());
			}
		});
		//System.out.println("Entering 1");
		int prevTokId = -1; 
		for (int id = 0; id < tokEntSort.size(); id++) {
			final int pointer = tokEntSort.getInt(id);
			final int tokId = tokScratch.get(pointer);
			final int entId = entScratch.getInt(pointer);
			final int entNewPos = entPosScratch.getInt(pointer);
			final int tokPos = tokPosScratch.getInt(pointer);
			
			if(prevTokId != tokId && prevTokId != -1) {
				writePreludeToPostingList(prevTokId);
				ent2OccurenceMap.clear();
				entOrderList.clear();
			}
			prevTokId = tokId;
			entOrderList.add(entId);
			if(ent2OccurenceMap.containsKey(entId))
				ent2OccurenceMap.get(entId).add(new int[]{entNewPos, tokPos});
			else
				ent2OccurenceMap.put(entId, new TIntArrayList(new int[]{entNewPos, tokPos}));
			
		}	
		//System.out.println("Exiting 1");
		if(prevTokId != -1) {
			writePreludeToPostingList(prevTokId);
			ent2OccurenceMap.clear();
			entOrderList.clear();
		}
		
		if (isCrowded()) {
			flushBatch();
		}
	}
	
	private void writePreludeToPostingList(final int tokId) {
		// calculate number of ints needed for this tokId and this document
		final int numEnts = ent2OccurenceMap.size();
		int nIntsToAlloc = 0;
		nIntsToAlloc += 1; // tokId
		nIntsToAlloc += 1; // numents
		nIntsToAlloc +=2 * numEnts + 2 * (entOrderList.size()); // For each entid, you have: entId, nposts, ...posts (entPos, tokPos)

		//allocate space and cursor
		int cursor = batchIntBuffer.size();
		batchIntBuffer.size(cursor + nIntsToAlloc);
		
		// write prelude 
		batchIntBuffer.set(cursor++, tokId);
		batchIntBuffer.set(cursor++, numEnts);

		int lastEntId = -1;
		for (int id = 0; id < entOrderList.size(); ++id) {
			final int entId = entOrderList.get(id);
			if(lastEntId==entId) continue;
			//Write the entityId
			batchIntBuffer.set(cursor++, entId);
			// write the number of posting entries to follow
			batchIntBuffer.set(cursor++, ent2OccurenceMap.get(entOrderList.get(id)).size());
			TIntArrayList tempList = ent2OccurenceMap.get(entOrderList.get(id));
			for(int i=0; i<tempList.size(); i++){
				batchIntBuffer.set(cursor++, tempList.get(i));
			}
			lastEntId = entId;
		}
	}
	
	private boolean isCrowded() {
		return batchIntBuffer.size() > 0.9d * MAX_BATCH_BUF_INTS;
	}
	
	/**
	 * Turn absolute positions into pgaps.
	 * Fill an indirection array to point to the first ints of document records
	 * in (tokId, docId) order.
	 */
	private void rewriteForFlush() {
		tokenIdSorter.clear();
		for (int cursor = 0, cMax = batchIntBuffer.size(); cursor < cMax; ) {
			tokenIdSorter.add(cursor);
			/* final int tokId = */ batchIntBuffer.getInt(cursor++);
			final int numEnts = batchIntBuffer.getInt(cursor++);
			MonitorFactory.add("numEnts", "", numEnts);
			for(int i=0; i<numEnts; i++){
				//int entId = 
				batchIntBuffer.getInt(cursor++);
				int postingsSize = batchIntBuffer.getInt(cursor++);
				cursor = cursor + postingsSize;
			}
			// ends one doc entry
		}
		Sorting.quickSort(tokenIdSorter.elements(), 0, tokenIdSorter.size(), new cern.colt.function.IntComparator() {
			@Override
			public int compare(int o1, int o2) {
				final int tok1 = batchIntBuffer.getInt(o1);
				final int tok2 = batchIntBuffer.getInt(o2);
				final int cmpTok = tok1 - tok2;
				return cmpTok;
			}
		});
	}
	
	public void flushBatch() throws IOException {
		reuse();
		rewriteForFlush();
		if (batchIntBuffer.isEmpty()) return;
		final int batchNumber = sharedBatchCounter.getAndIncrement();
		final File indexDir = new File(config.getString(iitb.CSAW.OtherSystems.PropertyKeys.indexDirKey));
		final File ssqteFile = new File(indexDir, fieldName + "_" + batchNumber + iitb.CSAW.OtherSystems.PropertyKeys.ssqteInterimExtension);
		final DataOutputStream ssqteDos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ssqteFile)));
		// verify and flush in permuted order
		final SSQEntityBlock idp = new SSQEntityBlock(); 
		for (int prevTokId = -1, bsx = 0, bsMax = tokenIdSorter.size(); bsx < bsMax; ++bsx) {
			int cursor = tokenIdSorter.getInt(bsx);
			final int tokId = batchIntBuffer.getInt(cursor++);
			final int numEnts = batchIntBuffer.getInt(cursor++);
			int tempCursor = cursor; entIdSorter.clear();
			for(int i=0; i<numEnts; i++){
				entIdSorter.add(tempCursor);
				tempCursor++;
				final int nposts = batchIntBuffer.getInt(tempCursor++);
				tempCursor += nposts;
			}
			Sorting.quickSort(entIdSorter.elements(), 0, entIdSorter.size(), new cern.colt.function.IntComparator() {
				@Override
				public int compare(int o1, int o2) {
					final int ent1 = batchIntBuffer.getInt(o1);
					final int ent2 = batchIntBuffer.getInt(o2);
					final int cmpTok = ent1 - ent2;
					return cmpTok;
				}
			});
			for(int i=0; i<entIdSorter.size(); i++){
				int entCursor = entIdSorter.getInt(i);
				final int entId = batchIntBuffer.getInt(entCursor++);
				final int nposts = batchIntBuffer.getInt(entCursor++);
				idp.load(batchIntBuffer, entCursor, tokId, entId, nposts);
				idp.store(ssqteDos);
				cursor += nposts+2;//cursor += nposts;
			}
			if (tokId < prevTokId) {
				throw new IllegalStateException("sort(catId,docId) mangled");
			}
		}
		ssqteDos.close();
		// collect stats
		final long ssqteFileLength = ssqteFile.length();
		final double bytesPerInt = 1d * ssqteFileLength / batchIntBuffer.size();
		final long now = System.currentTimeMillis();
//		final double batchDocsPerMs = 1d * nDocsInBatch / (now - batchBeginTime);
//		final double batchPostsPerMs = 1d * nBatchPosts / (now - batchBeginTime);
		final double avgDictSize = MonitorFactory.getMonitor("dictSize", "").getAvg();
		final double avgPgap = MonitorFactory.getMonitor("pgap", "").getAvg();
		logger.info("Batch #" + batchNumber + " ints=" + batchIntBuffer.size() + " byte/int=" + bytesPerInt + " avgDictSize=" + avgDictSize + " avgPgap=" + avgPgap);
		batchBeginTime = now;
		// clean out the RAM
		batchIntBuffer.clear();
		tokenIdSorter.clear();
		nDocsInBatch = 0;
	}
}
