package it.unimi.dsi.mg4j.tool;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import it.unimi.dsi.Util;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.io.ByteArrayPostingList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Level;

/**
 * A subclass of {@link ScanBatch} that is intended to quit after completing exactly one
 * batch and calling {@link Scan#dumpBatch} at the end of it. No {@link IDocument}
 * should be submitted to {@link SSQScanBatch} after it dumps the batch to disk. This
 * behavior is implemented to help multi-thread the indexing process.
 * @author ganesh
 */
public class SSQScanBatch extends ScanBatch {
	
	TIntIntHashMap atypeCountMap = new TIntIntHashMap(); 
	ACatalog catalog;
	HashMap<Integer, Scan> atype2ScanMap = new HashMap<Integer, Scan>();
	
	public SSQScanBatch(File runStorageDir, String fieldname, int batchNumber, TermProcessor termProcessor, ACatalog catalog, TIntIntHashMap atypeCountMap) throws FileNotFoundException {
		super(runStorageDir, fieldname, batchNumber, termProcessor);
		this.atypeCountMap = atypeCountMap;
		this.catalog = catalog;
		LOGGER.setLevel(Level.DEBUG);
		for (int atypeId : atypeCountMap.keys()) {
			String atypeName = fieldname+":"+catalog.catIDToCatName(atypeId);
			atype2ScanMap.put(atypeId, new Scan(atypeName, atypeName, Completeness.POSITIONS, termProcessor, IndexingType.REMAPPED, 0, 0, scanBufferSize, null, runStorageDir, batchNumber));
		}
	}
	
	public boolean scanOneDocument(IDocument idoc) throws IOException, ConfigurationException {
		final int docid = idoc.docidAsInt();
		MutableString tokenText = new MutableString();
		final int nTokens = idoc.numWordTokens();
		int tokenOffset = 0;
		TIntHashSet catIds = new TIntHashSet();
		for (; tokenOffset < nTokens; ++tokenOffset) {
			tokenText.replace(idoc.wordTokenAt(tokenOffset));
			//if (!termProcessor.processTerm(tokenText)) continue;
			
			final int entId = catalog.entNameToEntID(tokenText.toString());
			//System.out.println(tokenText+"<"+entId+">");
			if (entId < 0) continue;
			catIds.clear();
			catalog.catsReachableFromEnt(entId, catIds);
			
			for (int catId : catIds.toArray()) {
				if(!atype2ScanMap.containsKey(catId)) continue;
				Scan aScan = atype2ScanMap.get(catId);
				ByteArrayPostingList termBapl;
				if ( ( termBapl = aScan.termMap.get( tokenText ) ) == null ) {
					try {
						termBapl = new ByteArrayPostingList( new byte[ BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE ], indexingIsStandard, completeness);
						aScan.termMap.put( tokenText.copy(), termBapl );
					}
					catch( OutOfMemoryError e ) {
						outOfMemoryError = true;
						aScan.termMap.growthFactor( 1 );
					}
					aScan.numTerms++;
					if ( aScan.numTerms % TERM_REPORT_STEP == 0 ) LOGGER.info( "[" + Util.format( aScan.numTerms ) + " term(s)]" );
				}
				termBapl.setDocumentPointer(docid);
				termBapl.addPosition(tokenOffset);
				if ( termBapl.outOfMemoryError ) {
					outOfMemoryError = true;
				}
				aScan.numOccurrences++;
			}
		} // for-offset
		long overallTerms=0;
		for (int atypeId : atype2ScanMap.keySet()) {
			Scan aScan = atype2ScanMap.get(atypeId);
			++aScan.documentCount;
			if (tokenOffset > aScan.maxDocSize) {
				aScan.maxDocSize = tokenOffset;
			}
			// because the docids are remapped we have to write both true docid and size
			aScan.sizes.writeGamma(docid);
			aScan.sizes.writeGamma(tokenOffset);
			// the following is needed to make Merge work properly
			if ( docid > aScan.maxDocInBatch ) {
				aScan.maxDocInBatch = docid;
			}
			
			documentsInBatch++;
			overallTerms=overallTerms + aScan.numTerms;
		}
		long percAvailableMemory = Util.percAvailableMemory();
		boolean compacted = false;
		if ( !outOfMemoryError && percAvailableMemory < Scan.PERC_AVAILABLE_MEMORY_CHECK ) {
			LOGGER.info( "Starting compaction... (" + percAvailableMemory + "% available)" );
			compacted = true;
			Util.compactMemory();
			percAvailableMemory = Util.percAvailableMemory();
			LOGGER.info( "Compaction completed (" + percAvailableMemory + "% available)" );
		}
		if ( outOfMemoryError || overallTerms >= maxTerms || documentsInBatch == documentsPerBatch || ( compacted && percAvailableMemory < Scan.PERC_AVAILABLE_MEMORY_DUMP ) ) {
			if ( outOfMemoryError ) LOGGER.warn( "OutOfMemoryError during buffer reallocation: writing a batch of " + documentsInBatch + " documents" );
			else if ( overallTerms >= maxTerms ) LOGGER.warn( "Too many terms (" + overallTerms + "): writing a batch of " + documentsInBatch + " documents" );
			else if ( compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP ) LOGGER.warn( "Available memory below " + PERC_AVAILABLE_MEMORY_DUMP + "%: writing a batch of " + documentsInBatch + " documents" );
			finish();
			return false;
		}
		return true;
	}
	
	public void finish() throws ConfigurationException, IOException {
		long occurrences = 0;
		for (int atypeId : atype2ScanMap.keySet()) {
			Scan aScan = atype2ScanMap.get(atypeId);
			occurrences+=aScan.dumpBatch();
		}
//		openSizeBitStream();
		LOGGER.info( "Last set of batches indexed at " + Util.format( ( 1000. * occurrences ) / ( System.currentTimeMillis() - batchStartTime ) ) + " occurrences/s" );
		batchStartTime = System.currentTimeMillis();
		documentsInBatch = 0;
		outOfMemoryError = false;
	}
}
