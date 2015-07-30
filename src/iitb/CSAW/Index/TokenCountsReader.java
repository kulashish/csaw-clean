package iitb.CSAW.Index;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.util.SemiExternalGammaList;
import it.unimi.dsi.util.StringMap;
import it.unimi.dsi.util.StringMaps;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

/**
 * <p>Read global token counts as saved by {@link TokenCountsMerger}.  
 * The vocabulary is still local to the current disk stripe, and stored in the
 * {@link DiskBasedIndex#TERMMAP_EXTENSION} file saved by MG4J (merger).  But
 * the global count files have extensions
 * {@link PropertyKeys#tokenGlobalCfExtension} and
 * {@link PropertyKeys#tokenGlobalDfExtension}.  All these are loaded from the
 * {@link AStripeManager#myTokenIndexDiskDir()} directory.</p>
 * 
 * <p>A bit slow because many maps are semi external, but because this class will
 * be used only for the {@link Field#token} field, only a few tokens in the
 * query will be looked up. <em>Update:</em> If the disk files are very small,
 * they are loaded into RAM.</p>
 * 
 * <p>2011/01/05: The constructor now takes a token index disk dir instead of the
 * usual {@link Config} because we will want to load two different
 * {@link TokenCountsReader}s: one for the payload and one for the reference
 * corpus.</p>
 * 
 * @author soumen
 */

public class TokenCountsReader {
	final Logger logger = Logger.getLogger(getClass());
	final long maxCacheGammaSize = 10*1024*1024;
	final long maxBijSize = 100*1024*1024;
	final File tokenIndexDiskDir;
	final StringMap<? extends CharSequence> termMap;
	final StringIntBijection termBij;
	final InputBitStream termGcfIbs, termGdfIbs;
	/** Access to {@link SemiExternalGammaList} must be synchronized. */
	final SemiExternalGammaList termGcfSegl, termGdfSegl;
	final Properties indexProperties;
	
	public TokenCountsReader(URI tokenIndexUri) throws ConfigurationException, IllegalArgumentException, SecurityException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		this(new File(tokenIndexUri.getPath()));
	}
	
	public TokenCountsReader(File tokenIndexDiskDir) throws IOException, ClassNotFoundException, ConfigurationException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		logger.info(getClass().getSimpleName() + " loading from " + tokenIndexDiskDir);
		this.tokenIndexDiskDir = tokenIndexDiskDir;
		
		final File termBijFile = new File(tokenIndexDiskDir, Field.token + iitb.CSAW.Index.PropertyKeys.tokenBijExtension);
		if (termBijFile.canRead() && termBijFile.length() < maxBijSize) {
			termBij = (StringIntBijection) BinIO.loadObject(termBijFile);
			termMap = null;
		}
		else {
			final File termmapFile = new File(tokenIndexDiskDir, Field.token + DiskBasedIndex.TERMMAP_EXTENSION);
			final ImmutableExternalPrefixMap unTermMap = (ImmutableExternalPrefixMap) BinIO.loadObject(termmapFile.getAbsolutePath());
			final File termDumpFile = new File(tokenIndexDiskDir, Field.token + DiskBasedIndex.TERMDUMP_EXTENSION);
			if (termDumpFile.canRead()) { // unTermMap is not self contained
				unTermMap.setDumpStream(termDumpFile.getAbsolutePath());
			}
			termMap = StringMaps.synchronize(unTermMap);
			termBij = null;
		}
		
		final File termGcfFile = new File(tokenIndexDiskDir, Field.token + iitb.CSAW.Index.PropertyKeys.tokenGlobalCfExtension);
		if (termGcfFile.length() < maxCacheGammaSize) {
			termGcfIbs = new InputBitStream(BinIO.loadBytes(termGcfFile));
		}
		else {
			termGcfIbs = new InputBitStream(termGcfFile);
		}
		termGcfSegl = new SemiExternalGammaList(termGcfIbs);
		
		final File termGdfFile = new File(tokenIndexDiskDir, Field.token + iitb.CSAW.Index.PropertyKeys.tokenGlobalDfExtension);
		if (termGdfFile.length() < maxCacheGammaSize) {
			termGdfIbs = new InputBitStream(BinIO.loadBytes(termGdfFile));
		}
		else {
			termGdfIbs = new InputBitStream(termGdfFile);
		}
		termGdfSegl = new SemiExternalGammaList(termGdfIbs);

		indexProperties = new Properties(new File(tokenIndexDiskDir, Field.token + DiskBasedIndex.PROPERTIES_EXTENSION));
	}
	
	public void close() throws IOException {
		termGcfIbs.close();
		termGdfIbs.close();
	}
	
	public int vocabularySize() {
		return termBij.size();
	}
	
	/**
	 * @param term Usually stemmed.
	 * @param strict Should we barf if term is not mapped?
	 * @return term ID if mapped, -1 otherwise.
	 */
	public long mapTermToId(CharSequence term, boolean strict) {
		long ans = -1;
		if (termBij != null) {
			ans = termBij.containsKey(term)? termBij.getLong(term) : -1;
		}
		if (termMap != null) {
			ans = termMap.containsKey(term)? termMap.getLong(term) : -1;
		}
		if (strict && ans < 0) {
			throw new IllegalArgumentException("Cannot map term " + term);
		}
		assert ans <= Integer.MAX_VALUE;
		return ans;
	}
	
	public String mapIdToTerm(long termId) {
		return termBij.intToString((int) termId);
	}
	
	/**
	 * @param termId no bound check
	 * @return corpus frequency
	 */
	public synchronized long globalCorpusFrequency(long termId) {
		return termGcfSegl.getLong((int) termId);
	}
	
	public synchronized long globalCorpusFrequency(CharSequence term, boolean strict) {
		final long termId = mapTermToId(term, strict);
		return termId < 0? 0 : termGcfSegl.getLong((int) termId);
	}
	
	public long globalCorpusFrequency() {
		return indexProperties.getLong(iitb.CSAW.Index.PropertyKeys.globalNumOccurrences);
	}
	
	/**
	 * {@link #termGdfSegl} is not thread safe, hence synchronized
	 * @param termId no bound check
	 * @return document frequency
	 */
	public synchronized long globalDocumentFrequency(long termId) {
		return termGdfSegl.getLong((int) termId);
	}
	
	/**
	 * {@link #termGdfSegl} is not thread safe, hence synchronized
	 */
	public synchronized long globalDocumentFrequency(CharSequence term, boolean strict) {
		final long termId = mapTermToId(term, strict);
		return termId < 0? 0 : termGdfSegl.getLong((int) termId);
	}
	
	public long globalDocumentFrequency() {
		return indexProperties.getLong(iitb.CSAW.Index.PropertyKeys.globalMaxDocument1);
	}
	
	public double logIdf(long termId) {
		return Math.log(1d + ((double) globalDocumentFrequency()) / ((double) globalCorpusFrequency(termId)));
	}
	
	/**
	 * @param term
	 * @param strict
	 * @return If term is not in indexed vocabulary and strict=true, throws {@link IllegalArgumentException}.
	 * If strict=false, returns {@link Double#POSITIVE_INFINITY}.
	 */
	public double logIdf(CharSequence term, boolean strict) {
		final long termId = mapTermToId(term, strict);
		return termId < 0? Double.POSITIVE_INFINITY : logIdf(termId);
	}
}
