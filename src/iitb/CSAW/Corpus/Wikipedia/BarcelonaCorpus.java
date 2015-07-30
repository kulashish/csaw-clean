package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.document.IDocument;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Eventual replacement for {@link WikipediaCorpus}.  Uses Yahoo Barcelona
 * annotated Wikipedia as source feed.  Re-tokenizes from scratch without
 * depending on Yahoo Barcelona's tokenization.  Uses a monolithic Berkeley
 * DB {@link EntityStore} instead of a separate {@link RandomAccessFile}.
 * Eventually {@link WikipediaCorpus} will use standard MediaWiki feed.
 * @author soumen
 */
public class BarcelonaCorpus extends ACorpus {
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		if (args[2].equals("build")) {
			BarcelonaCorpus bcBuild = new BarcelonaCorpus(conf, false);
			bcBuild.build();
			bcBuild.close();
		}
		else if (args[2].equals("scan")) {
			BarcelonaCorpus bcScan = new BarcelonaCorpus(conf, true);
			bcScan.scan();
			bcScan.close();
		}
	}
	
	@Entity static class DocumentRecord {
		@PrimaryKey long key = -1;
		byte[] val = null;
	}

	public enum Column {
		token, stem, POS, morph, CONL, WNSS, WSJ, ana, head, deplabel, link, hyper
	}

	public static final String barcelonaDataDirName = "Barcelona.DataDir";
	public static final String barcelonaCorpusDirName = "Barcelona.CorpusDir";
	public static final String storeDirName = "Barcelona.EntityStoreDir";
	
	static final int linkCol = Column.link.ordinal();
	static final int tokenCol = Column.token.ordinal();

	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final Environment env;
	final EntityStore entityStore;
	final PrimaryIndex<Long, DocumentRecord> primaryIndex;
	final boolean readOnly, useCompression;
	
	transient Transaction txn = null;
	transient EntityCursor<DocumentRecord> cursor = null;
	static final int DOC_CAPACITY = 200000;
	static final int LINE_CAPACITY = 5000;
	
	public BarcelonaCorpus(Config conf) throws EnvironmentLockedException, DatabaseException {
		this(conf, true);
	}
	
	public BarcelonaCorpus(Config conf, boolean readOnly) throws EnvironmentLockedException, DatabaseException {
		this(conf, readOnly, false);
	}
	
	public BarcelonaCorpus(Config conf, boolean readOnly, boolean useCompression) throws EnvironmentLockedException, DatabaseException {
		super(conf, Field.none);
		this.conf = conf;
		this.readOnly = readOnly;
		this.useCompression = useCompression;
		final File envHome = new File(conf.getString(storeDirName));
		env = new Environment(envHome, ACorpus.getDefaultEnvironmentConfig(readOnly));
		entityStore = new EntityStore(env, getClass().getSimpleName(), ACorpus.getDefaultStoreConfig(readOnly));
		primaryIndex = entityStore.getPrimaryIndex(Long.class, DocumentRecord.class);
		reset();
	}
	
	public void close() throws DatabaseException 
	{
	   	if (cursor != null) {
			cursor.close();
			cursor = null;
		}
		if (txn != null) {
			txn.abort();
			txn = null;
		}
		entityStore.close();
		env.close();
	}
	
	public void reset() throws DatabaseException
	{
	 	if (cursor != null) {
			cursor.close();
			cursor = null;
		}
		if (txn != null) {
			txn.abort();
			txn = null;
		}
		TransactionConfig tc = new TransactionConfig();
		tc.setReadUncommitted(true);
		txn = env.beginTransaction(null, tc);
		CursorConfig cc = new CursorConfig();
		cc.setReadUncommitted(true);
		cursor = primaryIndex.entities(txn, cc);
	}
	
	public long numDocuments() throws DatabaseException
	{
	  	return primaryIndex.count();
	}
	
	public boolean nextDocument(BarcelonaDocument outBd) throws DatabaseException, IOException {
		DocumentRecord dr;
		synchronized (cursor) {
			dr = cursor.next();
		}
		if (dr == null) {
			return false;
		}
		final FastByteArrayInputStream fbais = new FastByteArrayInputStream(dr.val);
		final DataInputStream dis = new DataInputStream(fbais);
		outBd.load(dis);
		dis.close();
		return true;
	}
	
	public boolean getDocument(long docId, BarcelonaDocument outBd) throws DatabaseException, IOException {
		DocumentRecord dr = primaryIndex.get(docId);
		if (dr == null) {
			return false;
		}
		final FastByteArrayInputStream fbais = new FastByteArrayInputStream(dr.val);
		final DataInputStream dis = new DataInputStream(fbais);
		outBd.load(dis);
		dis.close();
		return true;
	}

	private void scan() throws Exception {
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.logInterval = ProgressLogger.TEN_SECONDS;
//		pl.expectedUpdates = pk.count();
		reset();
		final int nThreads = conf.getInt(Config.nThreadsKey);
		WorkerPool workerPool = new WorkerPool(this, nThreads);
		final AtomicLong nRecs = new AtomicLong(0);
		pl.start("Starting scan.");
		for (int tx = 0; tx < conf.getInt(Config.nThreadsKey); ++tx) {
			workerPool.add(new IWorker() {
				long wNumDone = 0;
				
				@Override
				public Exception call() throws Exception {
					BarcelonaDocument doc = new BarcelonaDocument();
					while (nextDocument(doc)) {
						nRecs.incrementAndGet();
						pl.update();
						++wNumDone;
					}
					return null;
				}
				
				@Override
				public long numDone() {
					return wNumDone;
				}
			});
		}
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
		pl.stop("Finished scan.");
		logger.info(nRecs + " records scanned");
	}
	
	private void build() throws DatabaseException, IOException {
		TransactionConfig tc = new TransactionConfig();
		tc.setNoSync(true);
		Transaction txn = env.beginTransaction(null, tc);

		long docIdGen = 0;
		BarcelonaDocument bdbd = new BarcelonaDocument();
		File srcDir = new File(conf.getString(BarcelonaCorpus.barcelonaDataDirName));
		File[] srcFiles = srcDir.listFiles((FileFilter) new RegexFileFilter("wiki\\d+\\.rebuild\\.txt(\\.gz)??"));
		Arrays.sort(srcFiles);
		ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = srcFiles.length;
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start("Starting transcription.");
		MutableString line = new MutableString(BarcelonaCorpus.LINE_CAPACITY);
		MutableString rawText = new MutableString(BarcelonaCorpus.DOC_CAPACITY);
		line.loose();
		rawText.loose();
		for (java.io.File srcFile : srcFiles) {
			FastBufferedReader fbr = new FastBufferedReader(new FileReader(srcFile));
			// skip the first two lines of the file
			fbr.readLine(line);
			fbr.readLine(line);
			for (;;) {
				line.length(0);
				if (fbr.readLine(line) == null) {
					if (rawText.trim().length() > 0) {
						bdbd.construct(++docIdGen, rawText);
						write(txn, bdbd);
						rawText.length(0);
					}
					break;
				}
				if (line.startsWith("%%#DOC")) {
					if (rawText.trim().length() > 0) {
						bdbd.construct(++docIdGen, rawText);
						write(txn, bdbd);
						rawText.length(0);
					}
				}
				rawText.append(line);
				rawText.append("\n"); // is this needed?
			}
			fbr.close();
			pl.update();
		}
		txn.commit();
		pl.stop("Finished transcription.");
	}
	
	private void write(Transaction txn, BarcelonaDocument bd) throws IOException, DatabaseException {
		FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(fbaos);
		bd.store(dos);
		dos.close();
		fbaos.trim();
		DocumentRecord dr = new DocumentRecord();
		dr.key = bd.docId;
		dr.val = fbaos.array;
		primaryIndex.putNoReturn(txn, dr);
	}

	static boolean isLink(MutableString link, String beginPattern, MutableString canon) {
		final boolean ans = link.startsWith(beginPattern);
		if (!ans) return ans;
		canon.length(0);
		canon.append(link.substring(beginPattern.length()));
		return ans;
	}

	public static boolean doAcceptLink(MutableString link, MutableString canon) {
		final boolean brc = isBeginLink(link, canon);
		if (brc) return brc;
		return isInLink(link, canon);
	}

	public static boolean isBeginLink(MutableString raw, MutableString canon) {
		return BarcelonaCorpus.isLink(raw, "B-/wiki/", canon);
	}

	public static boolean isInLink(MutableString raw, MutableString canon) {
		return BarcelonaCorpus.isLink(raw, "I-/wiki/", canon);
	}

	@Override
	public IDocument allocateReusableDocument() {
		throw new NotImplementedException();
	}

	@Override
	public boolean getDocument(long docid, IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		throw new NotImplementedException();
	}

	@Override
	public boolean nextDocument(IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		throw new NotImplementedException();
	}
}
