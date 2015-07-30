package iitb.CSAW.Corpus.Webaroo;

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
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import net.nutch.io.UTF8;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;

public class WebarooDocument implements IAnnotatedDocument {
	long docid;
	final MutableString url = new MutableString();
	final MutableString title = new MutableString();
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
		title.setLength(0);
		plainText.setLength(0);
		charOffsets.clear();
		cursor = 0;
	}
	
	public WebarooDocument() {
		clear();
	}

	public WebarooDocument(long docid, URL url, UTF8 title, MutableString plainText, IntArrayList charOffsets) {
		assign(docid, url, title, plainText, charOffsets);
	}
	
	public void assign(long docid, URL url, UTF8 title, MutableString plainText, IntArrayList charOffsets) {
		clear();
		this.docid = docid;
		if (url != null) {
			this.url.replace(url.toString());
		}
		if (title != null) {
			this.title.replace(title.toString());
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

	/**
	 * Word and non-word tokens strictly alternate, so we can divide the 
	 * internal token offset by 2 to report the word token offset.
	 * @param outOffset word token offset, half of internal token offset
	 * @param outWordText may be zero sized, will be mangled if return value is true
	 * @return if another word token was available
	 */
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
		storeMutableString(title, dos);
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
		loadMutableString(title, dis);
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
	 * In case of Webaroo we do not have reference annotations so we return
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
}
