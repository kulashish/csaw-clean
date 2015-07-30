package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Interval;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.mutable.MutableByte;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

public class WikiInRarDocument implements IAnnotatedDocument {

	public int docId;
	public String docName = null;
	private int tokens[];
	private int cursor = 0;
	private int numTokens = -1;
	private int numAnnotations = -1;

	private Collection<Annotation> annotations;

	public Collection<Annotation> getAnnotations() {
		return annotations;
	}

	WikiInRarDocument() {
		docId = -1;
		cursor = 0;
		tokens = null;
		numTokens = -1;
		numAnnotations = -1;
	}

	public WikiInRarDocument(IntArrayList tokens2,
			Collection<Annotation> annotations2) {
		this();
		numAnnotations = annotations2.size();
		this.annotations = annotations2;
		numTokens = tokens2.size();
		this.tokens = tokens2.toIntArray();
	}

	public void clear() {
		docId = -1;
		cursor = 0;
		tokens = null;
		numTokens = -1;
		numAnnotations = -1;
	}

	void load(int docId, ByteArrayList workingSpace) {

		this.docId = docId;
		if (docId >= 0) {
			this.docName = WikiInRarCorpus.entDictionary.intToString(docId);
		}
		MutableInt byteCursor = new MutableInt(0);
		MutableInt value = new MutableInt(0); // reusable
		MutableString entName = new MutableString(); // reusable
		loadWordTokens(workingSpace, byteCursor, value);
		loadAnnotations(workingSpace, byteCursor, value, entName);
	}

	/**
	 * loads the tokens present in the byte array into the {@link #tokens} array
	 * and resets the {@link #numTokens} of the current
	 * {@link #WikipediaDocument()}
	 * 
	 * @param workingSpace
	 * @param byteCursor
	 * @param value
	 *            reusbale int
	 */
	private void loadWordTokens(ByteArrayList workingSpace,
			MutableInt byteCursor, MutableInt value) {
		int tokenCount = 0;

		readInt(workingSpace, byteCursor, value);
		tokenCount = value.intValue();
		assert tokenCount >= 0 : "Error in reading :numTokens<0 for "
				+ " tokenCount " + tokenCount;

		readInt(workingSpace, byteCursor, value); // to read -1 as doc boundary
		tokens = new int[tokenCount];
		for (int ix = 0; ix < tokenCount; ix++) {
			readInt(workingSpace, byteCursor, value);
			tokens[ix] = value.intValue();
		}
		this.numTokens = tokenCount;
		readInt(workingSpace, byteCursor, value); // to read -1 as doc boundary
	}

	final static byte b4[] = new byte[4];

	private void readInt(ByteArrayList workingSpace, MutableInt byteCursor,
			MutableInt value) {
		for (int i = 0; i < 4; i++) {
			b4[i] = workingSpace.get(byteCursor.intValue());
			byteCursor.add(1);
		}
		value.setValue(convertByteToInt(b4[0], b4[1], b4[2], b4[3]));
	}

	/**
	 * Loads the annotations present in the byteArray in {@link #annotations}
	 * and sets the {@link #numAnnotations} for the current
	 * {@link #WikipediaDocument()}
	 * 
	 * @param workingSpace
	 * @param byteCursor
	 * @param value
	 *            reusable int
	 * @param entName
	 *            resuable string
	 */
	private void loadAnnotations(ByteArrayList workingSpace,
			MutableInt byteCursor, MutableInt value, MutableString entName) {

		entName.length(0);
		readInt(workingSpace, byteCursor, value);
		annotations = new ArrayList<Annotation>();
		int aCount = value.intValue();
		assert aCount >= 0 : "Error in reading :numAnnotations<0 for docID "
				+ docId;

		for (int ix = 0; ix < aCount; ix++) {

			readInt(workingSpace, byteCursor, value);
			assert value.intValue() >= 0 : "entID be positive"
					+ value.intValue();
			getEntName(value, entName);
			readInt(workingSpace, byteCursor, value);
			assert value.intValue() >= 0 : "left Index be positive"
					+ value.intValue();
			int left = value.intValue();
			readInt(workingSpace, byteCursor, value);
			assert value.intValue() >= 0 : "right Index be positive"
					+ value.intValue();
			int right = value.intValue();
			Interval interval = Interval.valueOf(left, right);
			annotations.add(new Annotation(entName.toString(), interval,
					(float) 0, 0));
		}
		this.numAnnotations = aCount;
	}

	private void getEntName(MutableInt value, MutableString entName) {
		entName.length(0);
		String eName = WikiInRarCorpus.entDictionary.intToString(value
				.intValue());
		entName.replace(eName);
	}

	@Override
	public long docidAsLong() {
		return docId;
	}

	@Override
	public boolean reset() {
		cursor = 0;
		return true;
	}

	@Override
	public boolean nextWordToken(MutableInt outOffset, MutableString outWordText) {
		outOffset.setValue(cursor);
		outWordText
				.replace(WikiInRarCorpus.tokenDictionary.intToString(cursor));
		cursor++;
		if (cursor >= numTokens)
			return false;
		return true;
	}

	@Override
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase) {
		for (int ix = span.left, right = span.right; ix < right; ix++) {
			phrase.add(WikiInRarCorpus.tokenDictionary.intToString(tokens[ix]));
		}
	}

	@Override
	public Collection<Annotation> getReferenceAnnotations() {
		return annotations;
	}

	@Override
	public int docidAsInt() {
		return docId;
	}

	@Override
	public int numWordTokens() {
		return numTokens;
	}

	@Override
	public CharSequence wordTokenAt(int wordPos) {
		if (wordPos < numTokens)
			return WikiInRarCorpus.tokenDictionary.intToString(tokens[wordPos]);
		else
			return null;
	}

	public void addAnnotation(Annotation a) {
		annotations.add(a);
	}

	public static int convertByteToInt(byte b3, byte b2, byte b1, byte b0) {
		return ByteBuffer.wrap(new byte[] { b3, b2, b1, b0 }).getInt();
	}

	@Override
	public String toString() {
		return "WikiInRarDocument [docId=" + docId + ", docName=" + docName
				+ ", tokens=" + Arrays.toString(tokens) + ", cursor=" + cursor
				+ ", numTokens=" + numTokens + ", numAnnotations="
				+ numAnnotations + ", annotations=" + annotations + "]";
	}

	public void write(ByteArrayOutputStream baos) {
		MutableByte byteArr[] = new MutableByte[4];
		for (int i = 0; i < 4; i++) {
			byteArr[i] = new MutableByte();
		}
		writeTokens(baos, byteArr);
		writeAnnotations(baos, byteArr);
		assert verify(baos) : "Error in writing to Byte Array";

	}

	private void writeAnnotations(ByteArrayOutputStream baos,
			MutableByte[] byteArr) {
		writeInt(baos, annotations.size(), byteArr);

		for (Annotation ix : annotations) {
			int eid = WikiInRarCorpus.getEntNameToId(ix.entName);
			if (eid < 0) {
				int redirectIndex = WikiInRarCorpus.redirectEntities
						.getInt(ix.entName);
				assert redirectIndex >= 0 : "redirect Index should be positive";
				eid = WikiInRarCorpus.redirectArr[redirectIndex];
			}
			assert eid >= 0 : "Entity ID  should be non negative";
			writeInt(baos, eid, byteArr);
			int left = ix.interval.left;
			assert left >= 0 : "Left index  should be non negative";
			writeInt(baos, left, byteArr);
			int right = ix.interval.right;
			assert right >= 0 : "Right index  should be non negative";
			writeInt(baos, right, byteArr);
		}
	}

	private void writeTokens(ByteArrayOutputStream baos, MutableByte[] byteArr) {

		writeInt(baos, tokens.length, byteArr);
		writeInt(baos, -1, byteArr); // to keep doc bounbdary

		for (int ix = 0, z = tokens.length; ix < z; ix++) {
			assert tokens[ix] >= 0 : "Token ID should be non negative";
			writeInt(baos, tokens[ix], byteArr);
		}
		writeInt(baos, -1, byteArr); // to keep doc bounbdary
	}

	private static void writeInt(ByteArrayOutputStream baos, int val,
			MutableByte[] b2) {
		convertIntToByte(val, b2);
		baos.write(b2[0].toByte());
		baos.write(b2[1].toByte());
		baos.write(b2[2].toByte());
		baos.write(b2[3].toByte());
	}

	static void convertIntToByte(int i, MutableByte b[]) {

		b[3].setValue((byte) (i & 0xff));
		i = i >> 8;
		b[2].setValue((byte) (i & 0xff));
		i = i >> 8;
		b[1].setValue((byte) (i & 0xff));
		i = i >> 8;
		b[0].setValue((byte) (i & 0xff));
		i = i >> 8;
	}

	public int[] getTokens() {
		return tokens;
	}

	public boolean verify(ByteArrayOutputStream baos) {

		WikiInRarDocument d1 = new WikiInRarDocument();
		d1.load(docId, new ByteArrayList(baos.toByteArray()));

		assert numTokens == d1.getTokens().length : "Tokens size do no match";
		assert numAnnotations == d1.getAnnotations().size() : "Annotations size do no match";

		Collection<Annotation> annotations2 = d1.getAnnotations();
		int[] tokens2 = d1.getTokens();
		for (int i = 0; i < numTokens; i++) {
			assert tokens[i] == tokens2[i] : "Token do no match at " + i
					+ "th position";
		}

		Annotation[] temp1 = annotations.toArray(new Annotation[annotations
				.size()]);
		Annotation[] temp2 = annotations2.toArray(new Annotation[annotations2
				.size()]);
		for (int i = 0; i < numAnnotations; i++) {
			assert temp1[i].equals(temp2[i]) : "Annotation do no match at " + i
					+ "th position";
		}
		return true;
	}

	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);

	public String[] toText(Config config) throws IOException,
			ClassNotFoundException {
		String[] words = new String[tokens.length];
		if (WikiInRarCorpus.tokenDictionary == null) {
			pl.start("Loading Token Dictionary");
			DataFactory.loadTokenDictionary(config);
			pl.done();
		}
		for (int ix = 0, z = tokens.length; ix < z; ix++) {
			words[ix] = WikiInRarCorpus.tokenDictionary.intToString(tokens[ix]);
		}
		return words;
	}

	public static void main(String[] args) {

		MutableByte byteArr[] = new MutableByte[4];
		for (int i = 0; i < 4; i++) {
			byteArr[i] = new MutableByte();
		}

		convertIntToByte(1000, byteArr);

		System.out.println(byteArr[0].toByte());
		System.out.println(byteArr[1].toByte());
		System.out.println(byteArr[2].toByte());
		System.out.println(byteArr[3].toByte());

		int i = convertByteToInt(byteArr[0].toByte(), byteArr[1].toByte(),
				byteArr[2].toByte(), byteArr[3].toByte());
		System.out.println(i);

		IntArrayList tokens = new IntArrayList();
		tokens.add(1);
		tokens.add(12);
		tokens.add(143);
		tokens.add(15);
		tokens.add(16);

		Collection<Annotation> anots = new ArrayList<Annotation>();

		WikiInRarDocument wrd1 = new WikiInRarDocument(tokens, anots);
		int size = (tokens.size() + 3 + anots.size() + 1) * 4;
		ByteArrayOutputStream b = new ByteArrayOutputStream(size);
		wrd1.write(b);
		System.out.println(wrd1);

		WikiInRarDocument t1 = new WikiInRarDocument();
		t1.load(1, new ByteArrayList(b.toByteArray()));
		System.out.println(t1);

	}
}