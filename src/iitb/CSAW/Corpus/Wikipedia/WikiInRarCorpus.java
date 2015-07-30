package iitb.CSAW.Corpus.Wikipedia;

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.THashSet;
import iitb.CSAW.Corpus.ACorpus;
import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.BufferedRaf;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.RAR;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.document.IDocument;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class WikiInRarCorpus extends ACorpus {

	static StringIntBijection tokenDictionary = null;
	static StringIntBijection entDictionary = null;
	static StringIntBijection redirectEntities = null;
	static int entStartOffset[] = null;
	static int redirectArr[] = null;
	static long offset[] = null;

	private final RAR rar;

	private long rarCurrPointer;
	int offsetsWritten[] = null;

	public void close() throws IOException, DatabaseException {
		rar.close();
		rfile.close();
	}

	public void reset() throws IOException {
		rar.reset();
	}

	public long numDocuments() throws DatabaseException {
		return rar.numRecords();
	}

	public boolean getDocument(long docId, WikiInRarDocument outDocument,
			ByteArrayList workingSpace) throws DatabaseException, IOException {
		if (!rar.readRecord(docId, workingSpace)) {
			return false;
		}
		fillAny((int) docId, workingSpace, outDocument);
		return true;
	}

	public boolean nextDocument(WikiInRarDocument outDocument,
			ByteArrayList workingSpace) throws DatabaseException, IOException {
		MutableLong outDocId = new MutableLong();
		if (!rar.nextRecord(outDocId, workingSpace)) {
			return false;
		}
		fillAny(outDocId.intValue(), workingSpace, outDocument);
		return true;
	}

	void fillAny(int docId, ByteArrayList workingSpace,
			final WikiInRarDocument outDocument) throws IOException {

		fillText(docId, workingSpace, outDocument);
	}

	void debugGetText(ByteArrayList workingSpace, MutableString docText)
			throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(
				workingSpace.elements(), 0, workingSpace.size());
		InflaterInputStream iis = new InflaterInputStream(bais);
		InputStreamReader isr = new InputStreamReader(iis);
		docText.length(0);
		for (int ch = -1; (ch = isr.read()) != -1;) {
			docText.append((char) ch);
		}
		isr.close();
	}

	void fillText(int docId, ByteArrayList workingSpace,
			WikiInRarDocument outDocument) throws IOException {
		outDocument.clear();
		outDocument.load(docId, workingSpace);
	}

	/**
	 * This is meant for writing out the corpus to RAR format, not for indexing
	 * or search.
	 * 
	 * @throws ClassNotFoundException
	 */
	private WikiInRarCorpus(Config config, boolean write) throws IOException,
			EnvironmentLockedException, DatabaseException,
			ClassNotFoundException {
		super(config, Field.none);
		this.rar = new RAR(new File(config.getString("wikiInRar")), write,
				write);
		config.setThrowExceptionOnMissing(true);

	}

	private Config config;

	public WikiInRarCorpus(Config config) throws IOException,
			EnvironmentLockedException, DatabaseException,
			ClassNotFoundException {

		super(config, Field.none);
		this.rar = new RAR(new File(config.getString("wikiInRar")), false,
				false);
		load(config);
		this.config = config;
		config.setThrowExceptionOnMissing(true);
	}

	private void appendRecord(long docId, IntArrayList tokens,
			Collection<Annotation> annotations) throws IOException {
		// +2 for text boundaries '-1'
		int docSize = (tokens.size() + 2 + 1) + (annotations.size() * 3 + 1);
		docSize = docSize * 4;

		ByteArrayOutputStream baos = new ByteArrayOutputStream(docSize);

		WikiInRarDocument temp = new WikiInRarDocument(tokens, annotations);
		temp.write(baos);

		baos.close();
		rar.appendRecord(docId, baos.toByteArray(), 0, baos.size());
		rarCurrPointer += (docSize + 12); // 12 docId(8),docLength(4)
	}

	/**
	 * @param args
	 *            [0]=/path/to/config [1]=/path/to/log [2]=opcode{build,verify}
	 *            [3]={verify=field, build=/path/to/raw/corpus}
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		args[2] = "test";
		WikiInRarCorpus wc = null;
		if ("buildTokenAndEntDictionary".equals(args[2])) {
			wc = new WikiInRarCorpus(config, false);
			wc.buildTokenAndEntDictionary(config);
		} else if ("buildRedirectMapping".equals(args[2])) {
			wc = new WikiInRarCorpus(config, false);
			wc.buildRedirectMapping(config);
		} else if ("buildRar".equals(args[2])) {
			wc = new WikiInRarCorpus(config, true);
			wc.buildRarAndEntityAnnotationsIndex(config);
		} else if ("buildAll".equals(args[2])) {
			wc = new WikiInRarCorpus(config, true);
			wc.buildTokenAndEntDictionary(config);
			wc.buildRedirectMapping(config);
			wc.buildRarAndEntityAnnotationsIndex(config);
		} else if ("test".equals(args[2])) {
			wc = new WikiInRarCorpus(config);

			WikiInRarDocument wrd = (WikiInRarDocument) wc
					.allocateReusableDocument();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					System.in));
			String line;
			while ((line = br.readLine()) != null) {
				wc.getDocument(line, wrd);
				ArrayList<String> a = wc.getAnnotations(line, 10, false);
				System.out.println("array of anchor texts "+Arrays.toString(wrd.toText(config)));
				System.out.println("array of sentences "+ a);
			}
		}
	}

	private void load(Config config) throws IOException, ClassNotFoundException {
		pl.start("Loading All data");
		DataFactory.loadAll(config);
		pl.done();

	}

	/**
	 * 
	 * @param config
	 * @param entName
	 * @param numAnnotations
	 * @param rFile
	 *            Random Access file reader which contains the RAR data
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static RandomAccessFile rfile = null;

	static byte buffer[] = null;

	public ArrayList<String> getAnnotations(String entName, int numAnnotations,
			boolean random) throws IOException, ClassNotFoundException {
		if (rfile == null) {
			rfile = new RandomAccessFile(config.getString("wikiInRar")
					+ "0.dat", "r");
		}
		if (buffer == null)
			buffer = new byte[100];
		int eId = entDictionary.getInt(entName);
		if (eId > 0) {
			return getALines(entStartOffset[eId], entStartOffset[eId + 1],
					numAnnotations, random);
		} else {
			System.err.println("Entity Not Present :(");
			return null;
		}
	}

	private ArrayList<String> getALines(int startIndex, int endIndex,
			int count, boolean random) throws IOException {

		ArrayList<String> annotations = new ArrayList<String>();

		// relocate endIndex as entries might be negative
		for (int ix = startIndex, z = endIndex; ix < z; ix++) {
			if (offset[ix] != -1) {
				endIndex = ix + 1;
			}
		}
		float p;
		Random obj = new Random();
		int required = count;
		ArrayList<String> aheadWords = new ArrayList<String>();
		ArrayList<String> backWords = new ArrayList<String>();
		StringBuilder sb = new StringBuilder();

		for (int ix = startIndex; ix < endIndex; ix++) {
			p = random ? (float) required / (endIndex - ix) : 1;
			if (obj.nextFloat() < p) {
				aheadWords.clear();
				backWords.clear();
				sb.delete(0, sb.length());
				required--;
				String line = getOneLine(offset[ix], buffer, rfile, aheadWords,
						backWords, sb);
				annotations.add(line.trim());
				if (required == 0)
					break;
			}
		}
		return annotations;
	}

	private String getOneLine(long offset, byte[] buffer,
			RandomAccessFile rfile, ArrayList<String> aheadWords,
			ArrayList<String> backWords, StringBuilder sb) throws IOException {

		int half = buffer.length / 2;
		long start = Math.max(0, offset - half);
		rfile.seek(start);
		rfile.readFully(buffer);
		for (int i = half; i < buffer.length - 4;) {
			int token = WikiInRarDocument.convertByteToInt(buffer[i],
					buffer[i + 1], buffer[i + 2], buffer[i + 3]);
			if (token == -1)
				break;
			assert token < tokenDictionary.size() : token
					+ " > tokenDictionary_size " + aheadWords.toString();
			aheadWords.add(tokenDictionary.intToString(token));
			i = i + 4;
		}

		for (int i = half - 4; i >= 0;) {
			int token = WikiInRarDocument.convertByteToInt(buffer[i],
					buffer[i + 1], buffer[i + 2], buffer[i + 3]);
			if (token == -1)
				break;
			backWords.add(tokenDictionary.intToString(token));
			i = i - 4;
		}

		for (int i = backWords.size() - 1; i >= 0; i--) {
			sb.append(backWords.get(i) + " ");
		}

//		sb.append("*");
		for (int i = 0; i < aheadWords.size(); i++) {
			sb.append(aheadWords.get(i) + " ");
		}
		return sb.toString();
	}

	public void getDocument(String entName, WikiInRarDocument wrd)
			throws DatabaseException, IOException, ClassNotFoundException {
		if (entDictionary == null) {
			DataFactory.loadEntDictionary(config);
		}

		if (entDictionary.containsKey(entName)) {
			int entId = entDictionary.getInt(entName);
			ByteArrayList b = new ByteArrayList();
			getDocument(entId, (IDocument) wrd, b);
		} else {
			System.err.println("Entity Not Present :(");
		}
	}

	private void buildTokenAndEntDictionary(Config config) throws IOException,
			JSONException {
		pl.start("Building Token and Entity Dictionary");
		WikiInRarCleanedDocProcesor ox = new WikiInRarCleanedDocProcesor(config);
		ox.buildTokensAndEntsFile(new File(config.getString("extractedWiki")));
		ox.buildTokensAndEntsSibj();
		pl.done();
	}

	private void buildRarAndEntityAnnotationsIndex(Config config)
			throws Exception {
		pl.start("Loading Data");
		if (WikiInRarCorpus.entDictionary == null)
			DataFactory.loadEntDictionary(config);
		if (WikiInRarCorpus.tokenDictionary == null)
			DataFactory.loadTokenDictionary(config);
		if (WikiInRarCorpus.redirectArr == null)
			DataFactory.loadRedirecArr(config);
		if (WikiInRarCorpus.redirectEntities == null)
			DataFactory.loadRedirectToEntitites(config);
		pl.done();

		TObjectIntHashMap<String> ent2Count = new TObjectIntHashMap<String>();
		LineIterator lit = new LineIterator(new FastBufferedReader(
				new FileReader(config.getString("cleanWikipediaWriteBase")
						+ "wikiEntAnnotationCount.txt")));

		pl.start("Building Ent Count Map");
		while (lit.hasNext()) {
			String line = lit.next().toString();
			String[] arr = line.split("\t");
			assert arr.length == 2 : " Error in reading File "
					+ Arrays.toString(arr);
			ent2Count.put(arr[0], Integer.parseInt(arr[1]));
		}
		pl.done();

		intializeOffsetArrays(ent2Count);
		build(config);
		close();
		storeOffsets(config);

	}

	private void buildRedirectMapping(Config config) throws IOException,
			ClassNotFoundException {

		// one time pass over 37gb wikipediadump.xml
		// createRedirectFile(config);

		if (entDictionary == null) {
			DataFactory.loadEntDictionary(config);
		}

		String redirectFile = config.getString("wikiRedirectFile");
		LineIterator lit = new LineIterator(new FastBufferedReader(
				new FileReader(redirectFile)));
		THashSet<String> seen = new THashSet<String>();
		pl.start("Creating unique ids");
		while (lit.hasNext()) {
			String line = lit.next().toString();
			String[] arr = line.split("\t");
			if (entDictionary.containsKey(arr[1])) {
				seen.add(arr[0]);
			}
			pl.update();
		}
		redirectEntities = new StringIntBijection(seen);
		pl.done();

		redirectArr = new int[redirectEntities.size()];
		LineIterator lit1 = new LineIterator(new FastBufferedReader(
				new FileReader(redirectFile)));
		pl.start("Creating redirect Mapping Array");
		while (lit1.hasNext()) {
			String line = lit1.next().toString();
			String[] arr = line.split("\t");
			int ix = redirectEntities.getInt(arr[0]);
			if (ix >= 0) {
				int toEnt = entDictionary.getInt(arr[1]);
				if (toEnt >= 0) {
					redirectArr[ix] = toEnt;
				}
			}
			pl.update();
		}
		pl.done();
		BinIO.storeObject(redirectArr, config.getString("wikiRedirectArr"));
		BinIO.storeObject(redirectEntities, config.getString("wikiRedirectEnt"));
	}

	private void storeOffsets(Config config) throws IOException {
		String base = config.getString("wikiInRar");
		pl.start("Persisting offsets array");
		BinIO.storeObject(offset, base + "offsets.ser");
		BinIO.storeObject(entStartOffset, base + "entStartOffset.ser");
		pl.done();
	}

	private void intializeOffsetArrays(TObjectIntHashMap<String> ent2Count) {
		pl.start("allocating offset buffers");
		offsetsWritten = new int[ent2Count.size()];
		entStartOffset = new int[ent2Count.size() + 1];
		int numEntities = entDictionary.size();
		int temp[] = new int[numEntities];
		TObjectIntIterator<String> iter = ent2Count.iterator();
		int eid = 0;
		while (iter.hasNext()) {
			iter.advance();
			String entName = iter.key();
			if (entDictionary.containsKey(entName))
				eid = entDictionary.getInt(entName);
			else if (redirectEntities.containsKey(entName))
				eid = redirectArr[redirectEntities.getInt(entName)];
			assert eid >= 0;
			temp[eid] += iter.value();
		}

		int sum = 0;
		for (int ix = 0; ix < temp.length; ix++) {
			entStartOffset[ix] = sum;
			offsetsWritten[ix] = 0;
			sum += temp[ix];
		}

		entStartOffset[numEntities] = sum;
		offset = new long[sum];
		for (int i = 0; i < sum; i++) {
			offset[i] = -1;
		}
		pl.done();
	}

	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);
	long docIdGen = 0;

	void build(Config conf) throws IOException, DatabaseException,
			JSONException {
		pl.start("starting compression");

		WikiInRarCleanedDocProcesor wc = new WikiInRarCleanedDocProcesor(conf);

		IntArrayList tokens = new IntArrayList();
		Collection<Annotation> annots = new ArrayList<Annotation>();

		MutableString entName = new MutableString();
		File dir = new File(conf.getString("extractedWiki"));
		for (File dx : dir.listFiles()) {
			File[] fileArr = dx.listFiles();
			for (File fx : fileArr) {
				LineIterator lit = new LineIterator(new FastBufferedReader(
						new FileReader(fx)));
				while (lit.hasNext()) {
					String lx = lit.next().toString();
					wc.getDocAndAnnotations(tokenDictionary, lx, entName,
							tokens, annots);
					updateEntityOccurenceOffsets(annots);
					flushPending(entName, tokens, annots);
				}
			}
		}
		pl.done();
	}

	FastBufferedReader openReader(java.io.File name)
			throws FileNotFoundException, IOException {
		if (name.getName().endsWith(".gz")) {
			return new FastBufferedReader(new InputStreamReader(
					new GZIPInputStream(new FileInputStream(name))));
		} else {
			return new FastBufferedReader(new InputStreamReader(
					new FileInputStream(name)));
		}
	}

	private void flushPending(MutableString entName, IntArrayList tokens,
			Collection<Annotation> annots) throws IOException {
		int eID = entDictionary.getInt(entName.toString());
		appendRecord(eID, tokens, annots);
		pl.update();
	}

	private void updateEntityOccurenceOffsets(Collection<Annotation> annots) {
		long currDocTextStartPointer = rarCurrPointer + 20;// 16
															// docId(8),docLength(4),TextLength,TextBoundary(-1)

		assert currDocTextStartPointer > 0;
		for (Annotation ix : annots) {
			String entName = ix.entName;
			int left = ix.interval.left;
			int id = getEntNameToId(entName);
			if (id == -1) {
				int rx = redirectEntities.getInt(entName);
				assert rx >= 0;
				id = redirectArr[rx];
			}

			assert id >= 0;
			int startIndex = entStartOffset[id];
			int written = offsetsWritten[id];

			int currIndex = startIndex + written;
			assert currIndex < entStartOffset[id + 1];

			long pointer = currDocTextStartPointer + left * 4;
			assert pointer >= 0;

			offset[currIndex] = pointer;
			offsetsWritten[id]++;
		}
	}

	static int getEntNameToId(String entName) {
		return WikiInRarCorpus.entDictionary.getInt(entName);
	}

	/**
	 * create Wikipedia redirects file. Running time is one pass over the
	 * orignial Wikipedia dump. Currently have commented the call in
	 * {@link #buildRedirectMapping(Config)}
	 * 
	 * @param props
	 * @throws IOException
	 */
	@SuppressWarnings("unused")
	private void createRedirectFile(Config props) throws IOException {
		String wikidump = props.getString("wikipediaDump");
		String redirectFile = props.getString("wikiRedirectFile");
		BufferedRaf rFile = new BufferedRaf(wikidump, "r", (int) 2e8);
		BufferedWriter bw = new BufferedWriter(new FileWriter(redirectFile));
		String title = null;
		String str, textTemp = null, titleTemp = null, text = null;
		while ((str = rFile.readLine()) != null) {
			if ((titleTemp = isTitle(str)) != null) {
				title = titleTemp;
			} else if ((textTemp = isRedirect(str)) != null) {
				text = textTemp;
				String line = title.replace(' ', '_') + "\t"
						+ text.replace(' ', '_') + "\n";
				bw.write(line);
			}
		}
		bw.close();
		rFile.close();
	}

	Pattern redirect = Pattern.compile("<redirect title=\"(.+)\".+");

	private String isRedirect(String line) {
		String result = null;
		Matcher matcher = redirect.matcher(line);
		if (matcher.find()) {
			result = matcher.group(1);
		}
		return result;
	}

	Pattern title = Pattern.compile("<title>(.+)</title>");

	private String isTitle(String line) {
		String result = null;
		Matcher matcher = title.matcher(line);
		if (matcher.find()) {
			result = matcher.group(1);
		}
		return result;
	}

	@Override
	public IAnnotatedDocument allocateReusableDocument() {
		return (IAnnotatedDocument) new WikiInRarDocument();
	}

	@Override
	public boolean getDocument(long docid, IDocument outDocument,
			ByteArrayList workingSpace) throws DatabaseException, IOException {
		return getDocument(docid, (WikiInRarDocument) outDocument, workingSpace);
	}

	@Override
	public boolean nextDocument(IDocument outDocument,
			ByteArrayList workingSpace) throws DatabaseException, IOException {
		return nextDocument((WikiInRarDocument) outDocument, workingSpace);
	}

	@SuppressWarnings("deprecation")
	public static void generateWikiEntitiesToCurrID(Config conf,
			String fileName, TObjectIntHashMap<String> wikiNameToCurrID)
			throws JSONException, IOException {

		if (wikiNameToCurrID != null)
			wikiNameToCurrID.clear();
		String id = "id";
		String url = "url";
		File dir = new File(conf.getString("extractedWiki"));
		BufferedWriter wikiNameToCurrIDWritere = null;

		wikiNameToCurrIDWritere = new BufferedWriter(new FileWriter(fileName));

		for (File dx : dir.listFiles()) {
			File[] fileArr = dx.listFiles();
			for (File fx : fileArr) {
				LineIterator lit = new LineIterator(new FastBufferedReader(
						new FileReader(fx)));
				while (lit.hasNext()) {
					String lx = lit.next().toString();
					JSONObject jsonObj = new JSONObject(lx);
					JSONArray jarr = (JSONArray) jsonObj.get(id);
					String ent = URLDecoder.decode(jsonObj.getString(url));
					int currID = Integer.parseInt(jarr.get(0).toString());
					if (wikiNameToCurrID != null) {
						wikiNameToCurrID.put(ent, currID);
					}
					wikiNameToCurrIDWritere.write(ent + "\t" + currID);
				}
			}
		}
		wikiNameToCurrIDWritere.close();
	}
}

class DataFactory {

	static void loadAll(Config config) throws IOException,
			ClassNotFoundException {
		if (WikiInRarCorpus.entDictionary == null)
			loadEntDictionary(config);
		if (WikiInRarCorpus.tokenDictionary == null)
			loadTokenDictionary(config);
		if (WikiInRarCorpus.redirectArr == null)
			loadRedirecArr(config);
		if (WikiInRarCorpus.redirectEntities == null)
			loadRedirectToEntitites(config);
		if (WikiInRarCorpus.entStartOffset == null)
			loadEntStartOffsetArr(config);
		if (WikiInRarCorpus.offset == null)
			loadOffsetArr(config);
	}

	static void loadEntDictionary(Config config) throws IOException,
			ClassNotFoundException {

		WikiInRarCorpus.entDictionary = (StringIntBijection) BinIO
				.loadObject(config.getString("cleanWikipediaWriteBase")
						+ "wikiEntNames.sibj");
	}

	static void loadTokenDictionary(Config config) throws IOException,
			ClassNotFoundException {
		WikiInRarCorpus.tokenDictionary = (StringIntBijection) BinIO
				.loadObject(config.getString("cleanWikipediaWriteBase")
						+ "wikiVocab.sibj");
	}

	static void loadRedirecArr(Config config) throws IOException,
			ClassNotFoundException {
		WikiInRarCorpus.redirectArr = (int[]) BinIO.loadObject(config
				.getString("wikiRedirectArr"));
	}

	static void loadRedirectToEntitites(Config config) throws IOException,
			ClassNotFoundException {
		WikiInRarCorpus.redirectEntities = (StringIntBijection) BinIO
				.loadObject(config.getString("wikiRedirectEnt"));
	}

	static void loadEntStartOffsetArr(Config config) throws IOException,
			ClassNotFoundException {
		WikiInRarCorpus.entStartOffset = (int[]) BinIO.loadObject(config
				.getString("entStartOffsetArr"));
	}

	static void loadOffsetArr(Config config) throws IOException,
			ClassNotFoundException {
		WikiInRarCorpus.offset = (long[]) BinIO.loadObject(config
				.getString("entOffsetArr"));
	}

}