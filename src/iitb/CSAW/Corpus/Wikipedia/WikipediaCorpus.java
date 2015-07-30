package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Catalog.YAGO.YagoCatalog;
import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus.Column;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.RAR;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.util.Interval;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

@Deprecated
public class WikipediaCorpus extends ACorpus {
	final Column wantedColumn;
	private final RAR rar;
	
	private transient ACatalog catalog;
	
	/**
	 * This constructor is for reading.
	 */
	public WikipediaCorpus(Config props, Field field) throws EnvironmentLockedException, IOException, DatabaseException {
		super(props, field);
		this.rar = new RAR(new File(props.getString(BarcelonaCorpus.barcelonaCorpusDirName)), false, false);
		switch (field) {
		case token:
			wantedColumn = Column.token;
			break;
		case ent:
			wantedColumn = Column.link;
			break;
		default:
			throw new IllegalArgumentException("Target field " + field + " not allowed");
		}
		// should not need a catalog here but this class in present form is going away anyway
		this.catalog = YagoCatalog.getInstance(props);
	}
	
	public void close() throws IOException, DatabaseException {
		rar.close();
	}
	
	public void reset() throws IOException {
		rar.reset();
	}
	
	public long numDocuments() throws DatabaseException {
		return rar.numRecords();
	}

	public boolean getDocument(long docId, WikipediaDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		if (!rar.readRecord(docId, workingSpace)) {
			return false;
		}
		fillAny(docId, workingSpace, outDocument);
		return true;
	}
	
	public boolean nextDocument(WikipediaDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		MutableLong outDocId = new MutableLong();
		if (!rar.nextRecord(outDocId, workingSpace)) {
			return false;
		}
		fillAny(outDocId.longValue(), workingSpace, outDocument);
		return true;
	}
	
	void fillAny(long docId, ByteArrayList workingSpace, final WikipediaDocument outDocument) throws IOException {
		switch (wantedColumn) {
		case token:
		case stem:
			fillText(docId, workingSpace, outDocument);
			break;
		case link:
		case hyper:
			fillHyperLink(docId, workingSpace, outDocument);
			break;
		default:
			throw new IllegalArgumentException("Field " + wantedColumn + " not supported");
		}
	}
	
	/**
	 * In the specific case of the Yahoo Barcelona Wikipedia corpus, since tokenization
	 * has been done and there is one token (position) per line, we do not need to look
	 * at the original text to generate link annotations. We just need to traverse type
	 * ancestors and fill the output document at the same offset.
	 * @param docId
	 * @param workingSpace
	 * @param outDocument
	 * @throws IOException
	 */
	void fillHyperLink(final long docId, ByteArrayList workingSpace, final WikipediaDocument outDocument) throws IOException {
		final boolean doHyper = wantedColumn == Column.hyper;
		final int column = (wantedColumn == Column.hyper)? Column.link.ordinal() : wantedColumn.ordinal();
		outDocument.docId = docId;
		CSVReader csvr = constructInflatedCSVReader(workingSpace);
		outDocument.clear();
		final MutableString candidateEntity = new MutableString(), canon = new MutableString();
		int tokenOffset = 0;
		for (String[] line = null; (line = csvr.readNext()) != null; ++tokenOffset) {
			if (checkFillTitleURL(line, outDocument)) {
				continue;
			}
			if (line.length < 1 + Column.link.ordinal() || line[column].equals("0")) {
				continue;
			}
			candidateEntity.replace(line[column].trim());
			if (BarcelonaCorpus.doAcceptLink(candidateEntity, canon)) {
				final int entid = catalog.entNameToEntID(canon.toString());
				if (entid >= 0) {
					addWordOrAnnotation(outDocument, tokenOffset, canon);
					if (doHyper) {
						TIntHashSet supCatIds = new TIntHashSet();
						catalog.catsReachableFromEnt(entid, supCatIds);
						final int tokenOffsetInner = tokenOffset;
						supCatIds.forEach(new TIntProcedure() {
							@Override
							public boolean execute(int supCatId) {
								canon.replace(catalog.catIDToCatName(supCatId));
								addWordOrAnnotation(outDocument, tokenOffsetInner, canon);
								return true;
							}
						});
					}
				}
			}
		}		
		csvr.close();
		checkConsistency(outDocument);
	}
	
	void debugGetText(ByteArrayList workingSpace, MutableString docText) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(workingSpace.elements(), 0, workingSpace.size());
		InflaterInputStream iis = new InflaterInputStream(bais);
		InputStreamReader isr = new InputStreamReader(iis);
		docText.length(0);
		for (int ch = -1; (ch = isr.read()) != -1; ) {
			docText.append((char) ch);
		}
		isr.close();
	}
	
	void fillText(long docId, ByteArrayList workingSpace, WikipediaDocument outDocument) throws IOException {
		final int column = wantedColumn.ordinal();
		outDocument.docId = docId;
//		MutableString docText = new MutableString();
//		debugGetText(workingSpace, docText);
		CSVReader csvr = constructInflatedCSVReader(workingSpace);
		outDocument.clear();
		MutableString word = new MutableString();
		int tokenOffset = 0;
		for (String[] line = null; (line = csvr.readNext()) != null; ++tokenOffset) {
			if (checkFillTitleURL(line, outDocument)) {
				continue;
			}
			if (line.length < 1 + Column.link.ordinal()) {
				continue;
			}
			word.replace(line[column]);
			word.trim();
			word.toLowerCase();
			if (word.length() == 0) {
				continue;
			}
			if (wantedColumn != Column.token && wantedColumn != Column.stem && word.equals("0")) {
				continue;
			}
			if (outDocument.textBuffer.length() > 0) {
				outDocument.textBuffer.append(' '); // only for plain text
			}
			addWordOrAnnotation(outDocument, tokenOffset, word);
		}
		csvr.close();
		checkConsistency(outDocument);
	}
	
	boolean checkFillTitleURL(String[] line, WikipediaDocument outDocument) {
		if (line.length >= 1 && line[0].startsWith("%%#PAGE ")) {
			final String titleUrl = line[0].substring("%%#PAGE ".length()).trim();
			outDocument.title.replace(titleUrl);
			outDocument.url.replace(titleUrl);
			return true;
		}
		else {
			return false;
		}
	}
	
	void addWordOrAnnotation(WikipediaDocument wd, int ofs, MutableString tok) {
		wd.wordTokenOffset.add(ofs);
		wd.wordTokenBeginAtChar.add(wd.textBuffer.length());
		wd.textBuffer.append(tok);
		wd.wordTokenEndAtChar.add(wd.textBuffer.length());
	}

	CSVReader constructInflatedCSVReader(ByteArrayList workingSpace) {
		ByteArrayInputStream bais = new ByteArrayInputStream(workingSpace.elements(), 0, workingSpace.size());
		InflaterInputStream iis = new InflaterInputStream(bais);
		InputStreamReader isr = new InputStreamReader(iis);
		CSVReader csvr = new CSVReader(isr, '\t', (char) 0);
		return csvr;
	}
	
	void checkConsistency(WikipediaDocument wd) {
		assert wd.wordTokenBeginAtChar.size() == wd.wordTokenEndAtChar.size();
		assert wd.wordTokenBeginAtChar.size() == wd.wordTokenOffset.size();
		// can do more...
	}
	
	/**
	 * This is meant for writing out the corpus to RAR format, not for indexing or search.
	 */
	WikipediaCorpus(Config props) throws IOException, EnvironmentLockedException, DatabaseException {
		super(props, Field.none);
		this.rar = new RAR(new File(props.getString(BarcelonaCorpus.barcelonaCorpusDirName)), true, true);
		wantedColumn = Column.stem;
		props.setThrowExceptionOnMissing(true);
		this.catalog = YagoCatalog.getInstance(props);
	}

	public void appendRecord(long docId, MutableString docText) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(docText.length());
		DeflaterOutputStream dos = new DeflaterOutputStream(baos);
		OutputStreamWriter osw = new OutputStreamWriter(dos);
		docText.write(osw);
		osw.close();
		dos.close();
		baos.close();
		rar.appendRecord(docId, baos.toByteArray(), 0, baos.size());
	}
	
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log
	 * [2]=opcode{build,verify}
	 * [3]={verify=field, build=/path/to/raw/corpus}
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		if ("build".equals(args[2])) {
			WikipediaCorpus wc = new WikipediaCorpus(config);
			wc.build(new File(args[3]));
			wc.close();
		}
		else if ("verify".equals(args[2])){
			WikipediaCorpus wc = new WikipediaCorpus(config, Field.valueOf(args[3]));
			wc.verify();
			wc.close();
		}
	}
	
	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);
	long docIdGen = 0;

	void build(File srcDir) throws IOException, DatabaseException {
		File[] srcFiles = srcDir.listFiles(new FilenameFilter() {
			@Override
			public boolean accept(java.io.File dir, String name) {
				return name.matches("wiki\\d+\\.rebuild\\.txt(\\.gz)??");
			}
		});
		Arrays.sort(srcFiles);

//		pl.expectedUpdates = srcFiles.length;
		pl.start("starting compression");
		MutableString line = new MutableString(BarcelonaCorpus.LINE_CAPACITY);
		MutableString name = new MutableString(BarcelonaCorpus.LINE_CAPACITY);
		MutableString doc = new MutableString(BarcelonaCorpus.DOC_CAPACITY);
		line.loose();
		name.loose();
		doc.loose();
		
//		srcFiles = Arrays.copyOfRange(srcFiles, 0, 2);
		
		for (java.io.File srcFile : srcFiles) {
			FastBufferedReader fbr = openReader(srcFile);
			// skip the first two lines of the file
			fbr.readLine(line);
			fbr.readLine(line);
			for (;;) {
				line.length(0);
				if (fbr.readLine(line) == null) {
					flushPending(name, doc);
					break;
				}
				if (line.startsWith("%%#DOC")) {
					flushPending(name, doc);
					String fields[] = line.toString().split("\\s+");
					name.replace(fields[1]);
				}
				doc.append(line);
				doc.append("\n"); // is this needed?
			}
			fbr.close();
//			pl.update();
		}
		pl.done();
	}
	
	FastBufferedReader openReader(java.io.File name) throws FileNotFoundException, IOException {
		if (name.getName().endsWith(".gz")) {
			return new FastBufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(name))));
		}
		else {
			return new FastBufferedReader(new InputStreamReader(new FileInputStream(name)));
		}
	}
	
	void flushPending(MutableString name, MutableString doc) throws FileNotFoundException, IOException, DatabaseException {
		if (name.length() > 0 && doc.length() > 0) { // write out 
			appendRecord(docIdGen++, doc);
			name.length(0);
			doc.length(0);
			pl.update();
		}
	}
	
	void verify() throws Exception {
		pl.expectedUpdates = rar.numRecords();
		pl.start();
		reset();
		final int nThreads = Runtime.getRuntime().availableProcessors();
		final AtomicLong nRecords = new AtomicLong(), sumRawLen = new AtomicLong();
		final WorkerPool workerPool = new WorkerPool(this, nThreads);
		for (int ntx = 0; ntx < nThreads; ++ntx) {
			workerPool.add(new IWorker() {
				long wNumDone = 0;
				
				@Override
				public Exception call() throws Exception {
					WikipediaDocument md = new WikipediaDocument();
					ByteArrayList work = new ByteArrayList();
					MutableString buf = new MutableString();
					while (nextDocument(md, work)) {
						logger.debug(md.docId + " " + md.title);
						nRecords.incrementAndGet();
						sumRawLen.addAndGet(md.textBuffer.length());
						verifyDetail(md, buf);
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
		pl.done();
		System.out.println(nRecords + " records, " + sumRawLen + " bytes of payload");
	}
	
	void verifyDetail(IAnnotatedDocument idoc, MutableString buf) {
		buf.length(0);
		idoc.reset();
		MutableInt outOffset = new MutableInt();
		MutableString outWordText = new MutableString();
		for (; idoc.nextWordToken(outOffset, outWordText); ) {
			buf.append(outOffset + "\t" + outWordText + "\n");
		}
	}

	@Override
	public IAnnotatedDocument allocateReusableDocument() {
		return new WikipediaDocument();
	}

	@Override
	public boolean getDocument(long docid, IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		return getDocument(docid, (WikipediaDocument) outDocument, workingSpace);
	}

	@Override
	public boolean nextDocument(IDocument outDocument, ByteArrayList workingSpace) throws DatabaseException, IOException {
		return nextDocument((WikipediaDocument) outDocument, workingSpace);
	}

	/**
	 * In case of Wikipedia Barcelona this job is simple because entity 
	 * annotations are non-overlapping.
	 * @param idoc
	 * @return
	 */
	static ReferenceArrayList<Annotation> docToAnnots(IAnnotatedDocument idoc) {
		final ReferenceArrayList<Annotation> ans = new ReferenceArrayList<Annotation>();
		final MutableString ent = new MutableString(), lastEnt = new MutableString();
		final MutableInt pos = new MutableInt();
		int beginEntPos = -1, lastEntPos = -1;
		for (idoc.reset(); idoc.nextWordToken(pos, ent); ) {
			final int ipos = pos.intValue();
			if (ent.equals(lastEnt) && ipos == lastEntPos + 1) {
				lastEntPos = ipos;
			}
			else {
				if (lastEnt.length() > 0) {
					final Annotation anno = new Annotation(lastEnt.toString(), Interval.valueOf(beginEntPos, lastEntPos), 0, 0);
					ans.add(anno);
				}
				beginEntPos = lastEntPos = ipos;
				lastEnt.replace(ent);
			}
		}
		return ans;
	}
}
