package iitb.CSAW.Corpus;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.mg4j.document.IDocument;

import java.io.IOException;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.StoreConfig;

/**
 * @author soumen
 */
public abstract class ACorpus {
	/**
	 * An implementation of {@link ACorpus} is not required to provide a view
	 * for the {@link Field#type}.
	 * @author soumen
	 */
	public enum Field { none, token, ent, type, };
	
	final Config config;
	final Field field;
	public static final long MAX_LOG_FILE_BYTES = 1L<<30;
	public static final int CACHE_PERCENT = 20;
	
	/**
	 * This forces subclasses to implement a standard constructor.
	 * @param config
	 */
	public ACorpus(Config config, Field field) {
		this.config = config;
		this.field = field;
	}

	public abstract long numDocuments() throws DatabaseException, IOException;
	/**
	 * Rewinds the document cursor. Note that there is only one cursor. 
	 */
	public abstract void reset() throws IOException, DatabaseException;
	/**
	 * Sequential access to a document.
	 * @param outDocument A mutable (for efficiency) document. Will be mangled in general.
	 * @param workingSpace Optional scratch area, can be null. Do not reuse before item is completely consumed.
	 * @return true if outDocument is valid, false if end of corpus was reached.
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public abstract boolean nextDocument(IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException;
	/**
	 * Random access to a document specified by an ID. Will generally involve seeks.
	 * @param docid
	 * @param outDocument A mutable (for efficiency) document. Will be mangled in general.
	 * @param workingSpace Optional scratch area, can be null. Do not reuse before item is completely consumed.
	 * @return true if outDocument is valid, false if docid was not found in the corpus.
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public abstract boolean getDocument(long docid, IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException;
	/**
	 * For better memory management, use this to get a mutable reusable document
	 * and pass this into {@link ACorpus#nextDocument(IDocument, ByteArrayList)} and
	 * {@link ACorpus#getDocument(long, IDocument, ByteArrayList)} to be filled.
	 * @return allocated document
	 */
	public abstract IDocument allocateReusableDocument();

	public static StoreConfig getDefaultStoreConfig(boolean readOnly) {
		final StoreConfig sc = new StoreConfig();
		sc.setAllowCreate(!readOnly);
		sc.setReadOnly(readOnly);
//		sc.setTransactional(readOnly);
//		sc.setDeferredWrite(!readOnly);
		sc.setTransactional(true);
		sc.setDeferredWrite(false);
		return sc;
	}

	public static EnvironmentConfig getDefaultEnvironmentConfig(boolean readOnly) {
		final EnvironmentConfig ec = new EnvironmentConfig();
		ec.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Long.toString(MAX_LOG_FILE_BYTES));
		ec.setAllowCreate(!readOnly);
		ec.setReadOnly(readOnly);
		ec.setTransactional(true);
		ec.setCachePercent(CACHE_PERCENT);
		return ec;
	}
}
