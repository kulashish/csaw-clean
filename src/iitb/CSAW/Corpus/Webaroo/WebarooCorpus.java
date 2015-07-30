package iitb.CSAW.Corpus.Webaroo;

import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.RAR;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.document.IDocument;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Provider of the Webaroo corpus.  Reads via libraries provided by Webaroo
 * and turns into our standard {@link ACorpus} representation.
 * 
 * 2011/02/10: starting to migrate to single entity store per disk stripe
 * rather than one per out_data file.  We add the new code, test, then delete
 * the old code.
 * 
 * @author soumen
 */
public class WebarooCorpus extends ACorpus {
	@Entity static class DocumentRecord {
		@PrimaryKey long key = -1;
		byte[] val = null;
	}
	
	final private RAR rar;
	
	public WebarooCorpus(Config conf, File baseDir, boolean doWrite, boolean doTruncate) throws IOException, EnvironmentLockedException, DatabaseException {
		super(conf, Field.token);
		this.rar = new RAR(baseDir, doWrite, doTruncate);
	}
	
	public WebarooCorpus(Config conf, File baseDir, boolean doWrite, boolean doTruncate, boolean onlyScan) throws IOException, EnvironmentLockedException, DatabaseException {
		super(conf, Field.token);
		this.rar = new RAR(baseDir, doWrite, doTruncate, onlyScan);
	}
	
	public void close() throws IOException, DatabaseException {
		rar.close();
	}
	
	public void reset() throws IOException {
		rar.reset();
	}
	
	public static boolean isReady(File baseDir) {
		return RAR.isReady(baseDir);
	}

	@Override
	public IAnnotatedDocument allocateReusableDocument() {
		return new WebarooDocument();
	}

	@Override
	public long numDocuments() throws DatabaseException, IOException {
		return rar.numRecords();
	}

	@Override
	public boolean getDocument(long docid, IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		final boolean ans = rar.readRecord(docid, workingSpace);
		if (ans) {
			FastByteArrayInputStream fbais = new FastByteArrayInputStream(workingSpace.elements(), 0, workingSpace.size());
			((WebarooDocument) outDocument).load(fbais);
		}
		return ans;
	}

	@Override
	public boolean nextDocument(IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		MutableLong outKey = new MutableLong();
		final boolean ans = rar.nextRecord(outKey, workingSpace);
		if (ans) {
			FastByteArrayInputStream fbais = new FastByteArrayInputStream(workingSpace.elements(), 0, workingSpace.size());
			((WebarooDocument) outDocument).load(fbais);
		}
		return ans;
	}

	/**
	 * Reduces synchronization by asking caller to provide storage.
	 * @param doc
	 * @param fbaos
	 * @throws IOException 
	 */
	public void appendDocument(WebarooDocument wdoc, FastByteArrayOutputStream localFbaos) throws IOException {
		localFbaos.reset();
		wdoc.store(localFbaos);
		localFbaos.flush();
		rar.appendRecord(wdoc.docid, localFbaos.array, 0, localFbaos.length);
	}

	/**
	 * Reader test harness.
	 * @param args [0]=/path/to/properties [1]=/path/to/rar
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], "/dev/null");
		Logger logger = Logger.getLogger(WebarooCorpus.class);
		ProgressLogger pl = new ProgressLogger(logger);
		
		File wrarDir = new File(args[1]);
		WebarooCorpus wrar = new WebarooCorpus(conf, wrarDir, false, false);
		pl.expectedUpdates = wrar.numDocuments();
		pl.logInterval = ProgressLogger.ONE_MINUTE;
		pl.start();
		WebarooDocument wdoc = (WebarooDocument) wrar.allocateReusableDocument();
		ByteArrayList workingSpace = new ByteArrayList();
		long numDocs = 0, numWords = 0, lastLog = System.currentTimeMillis();
		for (wrar.reset(); wrar.nextDocument(wdoc, workingSpace); ++numDocs) {
			logger.trace(wdoc.docidAsLong() + " " + wdoc.toString());
			final MutableInt wordOffset = new MutableInt();
			final MutableString wordText = new MutableString();
			int prevOffset = -1;
			for (wdoc.reset(); wdoc.nextWordToken(wordOffset, wordText); ++numWords) {
				if (prevOffset == wordOffset.intValue()) {
					logger.fatal("repeated offset " + prevOffset + " on docid " + wdoc.docidAsLong());
				}
				prevOffset = wordOffset.intValue();
			}
			final long now = System.currentTimeMillis();
			if (now - lastLog > ProgressLogger.ONE_MINUTE) {
				lastLog = now;
				System.out.println(numDocs + " docs " + numWords + " words " + String.format("%g", 1d*numWords/numDocs) + " words/doc");
			}
			pl.update();
		}
		pl.stop();
		pl.done();
		wrar.close();
	}
}
