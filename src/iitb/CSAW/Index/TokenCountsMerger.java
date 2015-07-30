package iitb.CSAW.Index;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.InputStreamLineIterator;
import iitb.CSAW.Utils.RemoteData;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.log4j.Logger;

/**
 * Given a set of {@link DiskBasedIndex#TERMS_EXTENSION},
 * {@link DiskBasedIndex#FREQUENCIES_EXTENSION} and 
 * {@link DiskBasedIndex#GLOBCOUNTS_EXTENSION} files from MG4J,
 * produce merged counts for the home disk stripe vocabulary.
 * To be run at the buddy leader of each disk stripe only,
 * <em>after</em>  {@link TokenIndexMerger} has completed.
 * @author soumen
 */
public class TokenCountsMerger extends BaseIndexMerger {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		TokenCountsMerger countMerger = new TokenCountsMerger(config);
		countMerger.mergeCounts();
		countMerger.mergeDocsOccsAcrossDiskStripes(countMerger.stripeManager, countMerger.field);
	}

	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final AStripeManager stripeManager;
	final Field field = Field.token;
	final String fieldName = field.toString();
	final URI tokenIndexDiskUri;

	TokenCountsMerger(Config config) throws IOException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, URISyntaxException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		if (stripeManager.myBuddyIndex() != 0) {
			throw new IllegalArgumentException("Run this only on each buddy master");
		}
		tokenIndexDiskUri = stripeManager.tokenIndexDiskDir(stripeManager.myDiskStripe());
		assert tokenIndexDiskUri.getHost().equals(stripeManager.myHostName());
	}
	
	class CountCursor {
		final InputBitStream freqIbs, globIbs;
		final InputStream termIs;
		final Iterator<String> iterator;
		int termIndex = 0;
		String term;
		long docFreq = -1, corpusFreq = -1;
		boolean available = false;
		final RemoteData rd;
		
		/** From local file system */
		CountCursor(File aTokenIndexDir) throws IOException {
			rd = null; // local
			final File termsFile = new File(aTokenIndexDir, fieldName + DiskBasedIndex.TERMS_EXTENSION);
			final File freqFile = new File(aTokenIndexDir, fieldName + DiskBasedIndex.FREQUENCIES_EXTENSION);
			final File globFile = new File(aTokenIndexDir, fieldName + DiskBasedIndex.GLOBCOUNTS_EXTENSION);
			logger.info("Opening " + termsFile + ", " + freqFile + ", " + globFile);
			freqIbs = new InputBitStream(freqFile);
			globIbs = new InputBitStream(globFile);
			termIs = new FileInputStream(termsFile);
			iterator = new InputStreamLineIterator(termIs);
			advance();
		}
		
		/** From remote host via Jsch 
		 * @throws Exception */
		CountCursor(URI aTokenIndexUri) throws Exception {
			rd = new RemoteData(aTokenIndexUri.getHost());
			final String termsPath = aTokenIndexUri.getPath() + File.separator + fieldName + DiskBasedIndex.TERMS_EXTENSION;
			final String freqPath = aTokenIndexUri.getPath() + File.separator + fieldName + DiskBasedIndex.FREQUENCIES_EXTENSION;
			final String globPath = aTokenIndexUri.getPath() + File.separator + fieldName + DiskBasedIndex.GLOBCOUNTS_EXTENSION;
			logger.info("Opening " + termsPath + ", " + freqPath + ", " + globPath);
			freqIbs = new InputBitStream(rd.getRemoteFileInputStream(freqPath));
			globIbs = new InputBitStream(rd.getRemoteFileInputStream(globPath));
			termIs = rd.getRemoteFileInputStream(termsPath);
			iterator = new InputStreamLineIterator(termIs);
			advance();
		}
		
		void advance() throws IOException {
			if (iterator.hasNext()) {
				available = true;
				term = iterator.next();
				docFreq = freqIbs.readLongGamma();
				corpusFreq = globIbs.readLongGamma();
				++termIndex;
			}
			else {
				available = false;
			}
		}
		
		void close() throws IOException {
			freqIbs.close();
			globIbs.close();
			termIs.close();
			if (rd != null) {
				rd.close();
			}
		}
	}
	
	void mergeCounts() throws Exception {
		final File tokenIndexDiskDir = new File(tokenIndexDiskUri.getPath());
		final File propFile = new File(tokenIndexDiskDir, fieldName + DiskBasedIndex.PROPERTIES_EXTENSION);
		final Properties props = new Properties(propFile);
		final int nTerms = props.getInt(Index.PropertyKeys.TERMS);

		final CountCursor myCursor = new CountCursor(new File(tokenIndexDiskUri.getPath()));
		final ReferenceArrayList<CountCursor> otherCursors = new ReferenceArrayList<CountCursor>();
		for (int diskStripe = 0; diskStripe < stripeManager.numDiskStripes(); ++diskStripe) {
			if (diskStripe == stripeManager.myDiskStripe()) {
				continue;
			}
			final URI otherTokenIndexDiskUri = stripeManager.tokenIndexDiskDir(diskStripe);
			final CountCursor aCursor = new CountCursor(otherTokenIndexDiskUri);
			otherCursors.add(aCursor);
		}
		
		final File gcfFile = new File(tokenIndexDiskDir, fieldName + iitb.CSAW.Index.PropertyKeys.tokenGlobalCfExtension);
		final OutputBitStream gcfLongs = new OutputBitStream(gcfFile);
		final File gdfFile = new File(tokenIndexDiskDir, fieldName + iitb.CSAW.Index.PropertyKeys.tokenGlobalDfExtension);
		final OutputBitStream gdfLongs = new OutputBitStream(gdfFile);
		
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = nTerms;
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start("Staring merge...");
		
		while (myCursor.available) {
			long outCf = myCursor.corpusFreq, outDf = myCursor.docFreq;
			for (CountCursor aCursor : otherCursors) {
				while (aCursor.available && aCursor.term.compareTo(myCursor.term) < 0) {
					aCursor.advance();
				}
				if (aCursor.available && aCursor.term.equals(myCursor.term)) {
					outCf += aCursor.corpusFreq;
					outDf += aCursor.docFreq;
				}
			}
			gcfLongs.writeLongGamma(outCf);
			gdfLongs.writeLongGamma(outDf);
			myCursor.advance();
			pl.update();
		}
		
		pl.stop("Done.");
		pl.done();
		
		gcfLongs.close();
		gdfLongs.close();
		myCursor.close();
		for (CountCursor aCursor : otherCursors) {
			aCursor.close();
		}
	}
}
