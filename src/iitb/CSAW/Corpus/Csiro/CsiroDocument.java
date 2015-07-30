package iitb.CSAW.Corpus.Csiro;

import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.tool.ScanBatch;
import it.unimi.dsi.util.Interval;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;
import org.jsoup.Jsoup;

public class CsiroDocument implements IAnnotatedDocument {
	long docid;
	final MutableString url = new MutableString();
	final MutableString plainText = new MutableString(ScanBatch.DEFAULT_BUFFER_SIZE);
	/**
	 * There is always an even number of tokens, with the first in every pair
	 * being a (possibly empty) word and the second a (possibly empty) non-word.
	 * Here we record the end char offset of word, nonword, word, ... etc.
	 */
	final IntArrayList charOffsets = new IntArrayList();
	int cursor;
	
	public void clear() {
		docid = -1;
		url.setLength(0);
		plainText.setLength(0);
		charOffsets.clear();
		cursor = 0;
	}
	
	public CsiroDocument() {
		clear();
	}

	public CsiroDocument(long docid, URL url, MutableString plainText, IntArrayList charOffsets) {	
		assign(docid, url, plainText, charOffsets);
	}
	
	public void assign(long docid, URL url, MutableString plainText, IntArrayList charOffsets) {
		clear();
		this.docid = docid;
		if (url != null) {
			this.url.replace(url.toString());
		}
		if (plainText != null) {
			this.plainText.replace(plainText);
		}
		this.charOffsets.addAll(charOffsets);
	}

	@Override
	public int docidAsInt() {
		if (docid >= Integer.MAX_VALUE) { 
			throw new IllegalStateException("docid=" + docid + "larger than Integer.MAX_VALUE");
		}
		return (int) docid;
	}

	@Override
	public long docidAsLong() {
		return docid;
	}

	@Override
	public boolean nextWordToken(MutableInt outOffset, MutableString outWordText) {
		outWordText.length(0);
		if (cursor >= charOffsets.size()) {
			return false;
		}
		final int cbeg = cursor == 0? 0 : charOffsets.get(cursor-1);
		final int cend = charOffsets.getInt(cursor);
		final char[] buf = plainText.array();
		outWordText.append(buf, cbeg, cend-cbeg);
		outOffset.setValue(cursor / 2);
		cursor += 2;
		return true;
	}

	@Override
	public boolean reset() {
		cursor = 0;
		return true;
	}
	
	/* Methods to serialize and deserialize */
	
	void store(OutputStream os) throws IOException {
		DeflaterOutputStream zos = new DeflaterOutputStream(os);
		DataOutputStream dos = new DataOutputStream(zos);
		dos.writeLong(docid);
		storeMutableString(url, dos);
		storeMutableString(plainText, dos);
		dos.writeInt(charOffsets.size());
		for (int tx = 0; tx < charOffsets.size(); ++tx) {
			dos.writeInt(charOffsets.getInt(tx));
		}
		dos.close();
	}
	
	void storeMutableString(MutableString ms, DataOutputStream dos) throws IOException {
		final int len = ms.length();
		dos.writeInt(len);
		final char[] buf = ms.array();
		for (int mx = 0; mx < len; ++mx) {
			dos.writeChar(buf[mx]);
		}
	}
	
	void loadMutableString(MutableString ms, DataInputStream dis) throws IOException {
		ms.length(0);
		final int len = dis.readInt();
		for (int cx = 0; cx < len; ++cx) {
			ms.append(dis.readChar());
		}
	}
	
	void load(InputStream is) throws IOException {
		clear();
		InflaterInputStream zis = new InflaterInputStream(is);
		DataInputStream dis = new DataInputStream(zis);
		docid = dis.readLong();
		loadMutableString(url, dis);
		loadMutableString(plainText, dis);
		final int tokenOffsetsSize = dis.readInt();
		for (int tx = 0; tx < tokenOffsetsSize; ++tx) {
			charOffsets.add(dis.readInt());
		}
		dis.close();
	}
	
	/* IDocument and IAnnotatedDocument compliance */
	
	@Override
	public int numWordTokens() {
		return charOffsets.size() / 2;
	}

	@Override
	public CharSequence wordTokenAt(int wordPos) {
		final int cursor = 2 * wordPos;
		final int cbeg = cursor == 0? 0 : charOffsets.get(cursor-1);
		final int cend = charOffsets.getInt(cursor);
		return plainText.subSequence(cbeg, cend);
	}

	@Override
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase) {
		throw new NotImplementedException();
	}
	
	/**
	 * In case of Csiro we do not have reference annotations so we return
	 * an empty iterator.
	 */
	@Override
	public Collection<Annotation> getReferenceAnnotations() {
		return new ReferenceArrayList<Annotation>();
	}
	
	/**
	 * For debugging only.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int tx = 0; tx < charOffsets.size(); ++tx) {
			final int cbeg = tx == 0? 0 : charOffsets.getInt(tx-1);
			final int cend = charOffsets.getInt(tx);
			if (tx % 2 == 0) {
				sb.append(plainText.substring(cbeg, cend));
			}
			else {
				for (int cx = cbeg; cx < cend; ++cx) {
					sb.append('_');
				}
			}
		}
		return sb.toString();
	}
	
	/**
	 * @param docId
	 * @param rawText in any case this will be "consumed" and emptied before return
	 * @return true iff a nontrivial document could be constructed
	 */
	public boolean construct(long docId, MutableString rawText) {
		clear();
		// check for proper mime type
		final String rawTextString = rawText.trim().toString();
		Pattern pattern = Pattern.compile("<DOC>.*<DOCHDR>\\s*(\\S+)\\s+.*Content-Type:\\s+(\\S+)\\s+.*</DOCHDR>(.*)</DOC>", Pattern.DOTALL);
		Matcher matcher = pattern.matcher(rawTextString);
		if (!matcher.find()) {
			rawText.length(0);
			return false;
		}
		final String url = matcher.group(1);
		final String mimeType = matcher.group(2);
		if (!mimeType.startsWith("text/html")) {
			rawText.length(0);
			return false;
		}
		this.docid = docId;
		this.url.append(url);
		final String bodyText = matcher.group(3);
		String textWithTagsStripped = Jsoup.parse(bodyText).text();
		StringTokenizer tokenizer = new StringTokenizer(textWithTagsStripped);		
		for (int chofs=0; ;) {
			if(!tokenizer.hasMoreTokens()){
				break;
			}
			String token = tokenizer.nextToken();
			chofs += token.length();
			charOffsets.add(chofs);
			chofs += 1;							
			charOffsets.add(chofs);
			plainText.append(token);
			plainText.append(" ");
		} // for-token
		//wdoc.assign(docId, url, title, plainText, offsets);
		rawText.length(0);
		return true;
	}
}
