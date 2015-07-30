package iitb.CSAW.Utils;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.apache.commons.lang.mutable.MutableLong;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Random access byte array records. Refinement of RAF_UTF_8.
 * @author soumen
 */
public class RAR {
	@Entity static class KeyOffset {
		@PrimaryKey long key = -1;
		long offset = -1;
		int size = -1;
		KeyOffset(long key, long offset, int size) {
			this.key = key;
			this.offset = offset;
			this.size = size;
		}
		KeyOffset() { }
	}

	static final String recordFileName = "0.dat", indexName = "index", readyName = "ready", storeName = "KeyOffset";
	final File baseDir, recordFile, indexDir, indexStamp;
	final DataOutputStream rard;
	final RandomAccessFile rafr;
	final DataInputStream rari;
	final EnvironmentConfig envConfig = new EnvironmentConfig();
	final Environment env;
	final StoreConfig storeConfig = new StoreConfig();
	final EntityStore es;
	final PrimaryIndex<Long, KeyOffset> espi;
	final int LEN = 16*(1<<20);
	private final byte[] tmpbuf = new byte[LEN];
	/** Can close at most once. */
	private boolean isClosed = true;
	final boolean onlyScan; 
	
	public RAR(File baseDir, boolean doWrite, boolean doTruncate) throws EnvironmentLockedException, IOException, DatabaseException {
		this(baseDir, doWrite, doTruncate, false);
	}
	
	/**
	 * @param baseDir
	 * @param doWrite
	 * @param doTruncate <b>Note</b> a database that is ready is never truncated
	 * @throws IOException
	 * @throws EnvironmentLockedException
	 * @throws DatabaseException
	 */
	public RAR(File baseDir, boolean doWrite, boolean doTruncate, boolean onlyScan) throws IOException, EnvironmentLockedException, DatabaseException {
		this.baseDir = baseDir;
		this.recordFile = new File(baseDir, recordFileName);
		this.indexDir = new File(baseDir, indexName);
		this.indexStamp = new File(baseDir, readyName);
		this.onlyScan = onlyScan;

		if (!indexDir.isDirectory() && !indexDir.mkdir()) {
			throw new IllegalStateException("Cannot open " + baseDir.getCanonicalPath() + " unless directory " + indexDir.getCanonicalPath() + " exists");
		}
		if (!doWrite && !indexStamp.exists()) {
			throw new IllegalStateException("Cannot open " + baseDir.getCanonicalPath() + " for reading unless " + indexStamp.getCanonicalPath() + " exists");
		}
		if (doWrite && indexStamp.exists() && !indexStamp.delete()) {
			throw new IOException("Cannot open " + baseDir.getCanonicalPath() + " for writing without deleting " + indexStamp.getCanonicalPath());
		}
		if (doWrite) {
			for (File indexFile : indexDir.listFiles()) {
				if (indexFile.isFile()) {
					if (!indexFile.delete()) {
						throw new IOException("Cannot open " + baseDir.getCanonicalPath() + " for writing without deleting " + indexFile.getCanonicalPath());
					}
				}
			}
		}
		envConfig.setAllowCreate(doWrite);
		envConfig.setReadOnly(!doWrite);
		storeConfig.setAllowCreate(doWrite);
		storeConfig.setReadOnly(!doWrite);
		env = new Environment(indexDir, envConfig);
		if (doWrite) {
			// we will reindex at close anyway
			String prefix = "persist#" + storeName + '#';
			for (String name : env.getDatabaseNames()) {
				if (name.startsWith(prefix)) {
					env.removeDatabase(null, name);
				}
			}
		}
		es = new EntityStore(env, storeName, storeConfig);
		espi = es.getPrimaryIndex(Long.class, KeyOffset.class);

		if (doWrite) {
			rafr = null;
			if (doTruncate && !indexStamp.exists()) {
				recordFile.delete();
			}
			rard = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(recordFile, !doTruncate), LEN));
			rari = null;
		}
		else {
			if (doTruncate) {
				throw new IllegalArgumentException("Cannot truncate in read-only mode");
			}
			rard = null;
			if (onlyScan) {
				rafr = null;
				rari = new DataInputStream(new FastBufferedInputStream(new FileInputStream(recordFile), LEN));
			}
			else {
				rafr = new RandomAccessFile(recordFile, "r");
				rari = null;
			}
		}
		isClosed = false;
	}
	
	private void checkClosed() {
		if (isClosed) {
			throw new IllegalStateException("RAR at " + baseDir + " is closed");
		}
	}

	public static boolean isReady(File baseDir) {
		return new File(baseDir, readyName).exists();
	}

	public synchronized void appendRecord(long key, byte[] value, int begin, int end) throws IOException {
		checkClosed();
		rard.writeLong(key);
		rard.writeInt(end-begin);
		rard.write(value, begin, end-begin);
	}
	
	public synchronized void close() throws IOException, DatabaseException {
		if (isClosed) {
			return;
		}
		if (rard != null) {
			rard.close();
			buildIndex();
		}
		if (rafr != null) {
			rafr.close();
		}
		if (rari != null) {
			rari.close();
		}
		es.close();
		env.close();
		isClosed = true;
	}
	
	synchronized void buildIndex() throws IOException, DatabaseException {
		checkClosed();
		RandomAccessFile localRaf = new RandomAccessFile(recordFile, "r");
		localRaf.seek(0);
		int numRecords = 0, maxLen = 0;
		ProgressLogger pl = new ProgressLogger();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start("Started building index.");
		for (long offset = 0; (offset = localRaf.getFilePointer()) < localRaf.length(); ) {
			final long key = localRaf.readLong();
			final int len = localRaf.readInt();
			for (int skip = len; skip > 0; ) {
				skip -= localRaf.skipBytes(skip);
			}
			KeyOffset ko = new KeyOffset(key, offset, len);
			espi.putNoReturn(ko);
			++numRecords;
			pl.update();
			maxLen = maxLen < len? len : maxLen;
		}
		pl.stop("Finished building index.");
		pl.done();
		localRaf.close();
		System.out.println(numRecords + " records");
		if (!indexStamp.createNewFile()) {
			throw new IOException("Could not create " + indexStamp.getCanonicalPath());
		}
	}
	
	/**
	 * Create an index for a barrel that already exists.
	 * @param baseDir
	 * @throws DatabaseException 
	 * @throws IOException 
	 * @throws EnvironmentLockedException 
	 */
	static void buildIndex(File baseDir) throws EnvironmentLockedException, IOException, DatabaseException {
		final RAR rar = new RAR(baseDir, true, false);
		rar.close();
	}
	
	public long numRecords() throws DatabaseException {
		checkClosed();
		return espi.count();
	}
	
	public synchronized void reset() throws IOException {
		checkClosed();
		rafr.seek(0);
	}
	
	public synchronized boolean nextRecord(MutableLong outKey, ByteArrayList outBuffer) throws IOException {
		checkClosed();
		if (onlyScan) {
			try {
				final long key = rari.readLong();
				final int len = rari.readInt();
				fillSequential(len, outBuffer);
				outKey.setValue(key);
				return true;
			}
			catch (EOFException eofx) {
				return false;
			}
		}
		else {	
			if (rafr.getFilePointer() >= rafr.length()) {
				return false;
			}
			final long key = rafr.readLong();
			final int len = rafr.readInt();
			fillRandom(len, outBuffer);
			outKey.setValue(key);
			return true;
		}
	}
	
	private void fillSequential(int len, ByteArrayList outBuffer) throws IOException {
		outBuffer.ensureCapacity(len);
		outBuffer.clear();
		for (int done = 0; done < len; ) {
			final int doing = rari.read(tmpbuf, 0, Math.min(tmpbuf.length, len-done));
			outBuffer.addElements(done, tmpbuf, 0, doing);
			done += doing;
		}
	}

	public synchronized boolean readRecord(long key, ByteArrayList outBuffer) throws DatabaseException, IOException {
		checkClosed();
		if (rari != null || rafr == null) {
			throw new IllegalStateException("Cannot readRecord in scan mode.");
		}
		KeyOffset ko = espi.get(key);
		if (ko == null) {
			return false;
		}
		rafr.seek(ko.offset);
		final long keyBack = rafr.readLong();
		if (keyBack != key) {
			throw new IOException("Sequence database corrupted " + keyBack + " != " + key);
		}
		final int len = rafr.readInt();
		fillRandom(len, outBuffer);
		return true;
	}

	private void fillRandom(int len, ByteArrayList outBuffer) throws IOException {
		outBuffer.ensureCapacity(len);
		outBuffer.clear();
		for (int done = 0; done < len; ) {
			final int doing = rafr.read(tmpbuf, 0, Math.min(tmpbuf.length, len-done));
			outBuffer.addElements(done, tmpbuf, 0, doing);
			done += doing;
		}
	}
	
	public static void randomTest() throws IOException, EnvironmentLockedException, DatabaseException {
		File baseDir = new File(System.getProperty("java.io.tmpdir"), "rar");
		baseDir.mkdir();
		RAR rar = new RAR(baseDir, true, false);
		byte[] buf = new byte[1000];
		Random random = new Random();
		random.nextBytes(buf);
		ByteArrayList bal = new ByteArrayList(buf);
		rar.appendRecord(1, buf, 0, 1 + random.nextInt(bal.size()));
		rar.appendRecord(1, buf, 0, 1 + random.nextInt(bal.size()));
		rar.close();
	}
	
	public static void scanTest(File baseDir) throws Exception {
		final RAR rar = new RAR(baseDir, false, false);
		final MutableLong key = new MutableLong();
		final ByteArrayList rec = new ByteArrayList();
		final ProgressLogger pl = new ProgressLogger();
//		pl.expectedUpdates = rar.numRecords();
		pl.logInterval = ProgressLogger.ONE_MINUTE/2;
		pl.start("Started scanning " + baseDir);
		while (rar.nextRecord(key, rec)) {
			pl.update();
		}
		pl.stop("Finished scanning " + baseDir);
		pl.done();
		rar.close();
	}
	
	/**
	 * @param args[0] = /path/to/base/dir
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final File baseDir = new File(args[0]);
		scanTest(baseDir);
	}
}
