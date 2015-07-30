package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

/**
 * Eventual replacement for {@link WikipediaDocument}.  Uses Yahoo Barcelona
 * annotated Wikipedia as source feed.  Re-tokenizes from scratch without
 * depending on Yahoo Barcelona's tokenization.
 * Eventually {@link WikipediaDocument} will use standard MediaWiki feed.
 * @author soumen
 */
public class BarcelonaDocument implements IRecord, IAnnotatedDocument {
	static final MutableString empty = new MutableString();
	
	static final char columnEndChar = '\t';
	static final String metaTokenPrefix = "%%#";
	
	/* Members */
	static Logger logger = Logger.getLogger(BarcelonaDocument.class);
	long docId;
	/** Records the %%#PAGE field which is the Wikipedia entity name.  Note this is mutable and public. */
	public final MutableString docTitle = new MutableString();
	final MutableString cleanText = new MutableString();
	final IntArrayList wordsNonWordsBeginCharPosInCleanText = new IntArrayList();
	private transient int wnwIndex = 0; 
	final ReferenceArrayList<Annotation> referenceAnnotations = new ReferenceArrayList<Annotation>();

	@Override
	public void store(DataOutput doi) throws IOException {
		doi.writeLong(docId);
		docTitle.writeSelfDelimUTF8(doi);
		cleanText.writeSelfDelimUTF8(doi);
		final int wnwSize = wordsNonWordsBeginCharPosInCleanText.size(); 
		doi.writeInt(wnwSize);
		for (int wnw : wordsNonWordsBeginCharPosInCleanText) {
			doi.writeInt(wnw);
		}
		doi.writeInt(referenceAnnotations.size());
		for (Annotation ra : referenceAnnotations) {
			doi.writeUTF(ra.entName);
			doi.writeInt(ra.interval.left);
			doi.writeInt(ra.interval.right);
		}
		doi.writeLong(docId); // lame error check
	}
	
	@Override
	public void load(DataInput dii) throws IOException {
		docId = dii.readLong();
		docTitle.length(0);
		docTitle.readSelfDelimUTF8(dii);
		cleanText.length(0);
		cleanText.readSelfDelimUTF8(dii);
		final int wnwSize = dii.readInt();
		wordsNonWordsBeginCharPosInCleanText.clear();
		for (int wx = 0; wx < wnwSize; ++wx) {
			wordsNonWordsBeginCharPosInCleanText.add(dii.readInt());
		}
		final int annoSize = dii.readInt();
		referenceAnnotations.clear();
		for (int ax = 0; ax < annoSize; ++ax) {
		    	final String entName = URLDecoder.decode(dii.readUTF(), "UTF-8");
			final int ileft = dii.readInt();
			final int iright = dii.readInt();
			referenceAnnotations.add(new Annotation(entName, Interval.valueOf(ileft, iright), 0, 0));
		}
		if (dii.readLong() != docId) {
			throw new StreamCorruptedException("docId mismatch");
		}
	}

	@Override
	public <IR extends IRecord> void replace(IR src) {
		final BarcelonaDocument bd = (BarcelonaDocument) src;
		this.docId = bd.docId;
		this.docTitle.replace(bd.docTitle);
		this.cleanText.replace(bd.cleanText);
		this.wordsNonWordsBeginCharPosInCleanText.clear();
		this.wordsNonWordsBeginCharPosInCleanText.addAll(bd.wordsNonWordsBeginCharPosInCleanText);
		this.referenceAnnotations.clear();
		this.referenceAnnotations.addAll(bd.referenceAnnotations);
	}

	/* Temporary helper storage --- makes this class multithread-unsafe */
	private transient final ReferenceArrayList<Annotation> charPosAnnotations = new ReferenceArrayList<Annotation>();
	private transient final MutableString renderedAnnotations = new MutableString();
	private transient final MutableString word = new MutableString(), nonWord = new MutableString();
	private transient final MutableString line = new MutableString(), tentativeToken = new MutableString();
	private transient final MutableString rawLink = new MutableString(), cleanLink = new MutableString(), entName = new MutableString();
	private transient final ReferenceArrayList<CharSequence> columns = new ReferenceArrayList<CharSequence>();	

	private void clear() {
		docId = -1;
		docTitle.length(0);
		cleanText.length(0);
		wordsNonWordsBeginCharPosInCleanText.clear();
		referenceAnnotations.clear();
		
		charPosAnnotations.clear();
		renderedAnnotations.length(0);
		word.length(0);
		nonWord.length(0);
		line.length(0);
		tentativeToken.length(0);
		rawLink.length(0);
		cleanLink.length(0);
		entName.length(0);
		columns.clear();
	}
	
	/**
	 * Concat all tokens and tokenize again for consistency. 
	 * Note we will record all annotations even with entNames not in our catalog.
	 * @throws IOException 
	 */
	void construct(long docId, MutableString rawText) throws IOException {
		clear();
		this.docId = docId;
		setDocTitle(rawText);
		prepareCleanTextAndCharAnnots(rawText, cleanText, charPosAnnotations);
		tokenizeAfresh(cleanText, wordsNonWordsBeginCharPosInCleanText);
		alignAnnotationsWithTokens(docId, cleanText, wordsNonWordsBeginCharPosInCleanText, charPosAnnotations, referenceAnnotations);
//		renderTokenAnnotations(cleanText, wordsNonWordsBeginCharPosInCleanText, referenceAnnotations, renderedAnnotations);
	}
	
	private void setDocTitle(MutableString rawText) throws UnsupportedEncodingException {
		Pattern pat = Pattern.compile("^\\%\\%\\#PAGE\\s+(\\S.*)$", Pattern.MULTILINE);
		Matcher mat = pat.matcher(rawText);
		if (mat.find()) {
			docTitle.length(0);
			// should not URLDecoder.decode here!
			docTitle.append(mat.group(1));
		}
	}

	static void renderTokenAnnotations(MutableString cleanText, IntArrayList wordsNonWordsBeginCharPosInCleanText, ReferenceArrayList<Annotation> referenceAnnotations, MutableString ans) {
		ans.length(0);
		for (Annotation refAnnot : referenceAnnotations) {
			ans.append(refAnnot.entName + " : ");
			final int wnwLeft = refAnnot.interval.left * 2;
			final int wnwRight = refAnnot.interval.right * 2;
			ans.append(cleanText.subSequence(wordsNonWordsBeginCharPosInCleanText.get(wnwLeft), wordsNonWordsBeginCharPosInCleanText.get(wnwRight+1)));
			ans.append("\n");
		}
		ans.length();
	}
	
	static void alignAnnotationsWithTokens(long docId, MutableString cleanText, IntArrayList wordsNonWordsBeginCharPosInCleanText, ReferenceArrayList<Annotation> charPosAnnotations, ReferenceArrayList<Annotation> referenceAnnotations) {
		// align with char pos annots
		final int maxPos2 = wordsNonWordsBeginCharPosInCleanText.size() - 1;
		int pos2 = 0;
		for (Annotation cpa : charPosAnnotations) {
			// find word or non-word that contains cpa.left
			for (;;) {
				final int wnwBegin = wordsNonWordsBeginCharPosInCleanText.getInt(pos2);
				final int wnwEnd = pos2 >= maxPos2? cleanText.length() - 1 : wordsNonWordsBeginCharPosInCleanText.getInt(pos2+1) - 1;
				if (wnwBegin <= cpa.interval.left && cpa.interval.left <= wnwEnd) {
					break;
				}
				else {
					++pos2;
				}
			}
			final int pos2begin = pos2;
			// find word or non-word that contains cpa.right
			for (; pos2 < wordsNonWordsBeginCharPosInCleanText.size();) {
				final int wnwBegin = wordsNonWordsBeginCharPosInCleanText.getInt(pos2);
				final int wnwEnd = pos2 >= maxPos2? cleanText.length() - 1 : wordsNonWordsBeginCharPosInCleanText.getInt(pos2+1) - 1;
				if (wnwBegin <= cpa.interval.right && cpa.interval.right <= wnwEnd) {
					break;
				}
				else {
					++pos2;
				}
			}
			final int pos2end = pos2;
			// round to nearest word (not non-word)
			final int tokBegin = (pos2begin % 2 == 0)? pos2begin : pos2begin + 1;
			final int tokEnd = (pos2end % 2 == 0)? pos2end : pos2end - 1;
			if (tokBegin <= tokEnd) {
				final Annotation tokAnnot = new Annotation(cpa.entName, Interval.valueOf(tokBegin/2, tokEnd/2), 0, 0);
				referenceAnnotations.add(tokAnnot);
			}
			else {
//				logger.warn("Could not align " + cpa + " in docId=" + docId + " _" + cleanText.subSequence(cpa.interval.left, cpa.interval.right) + "_");
			}
		}		
	}
	
	void tokenizeAfresh(MutableString cleanText, IntArrayList wordsNonWordsBeginCharPosInCleanText) throws IOException {
		final FastBufferedReader cleanFbr = new FastBufferedReader(cleanText);
		int atWordCharPos = 0;
		while(cleanFbr.next(word, nonWord)) {
			final int atNonWordCharPos = atWordCharPos + word.length();
			wordsNonWordsBeginCharPosInCleanText.add(atWordCharPos);
			wordsNonWordsBeginCharPosInCleanText.add(atNonWordCharPos);
			atWordCharPos += word.length() + nonWord.length();
		}
		cleanFbr.close();
	}
	
	void prepareCleanTextAndCharAnnots(MutableString rawText, MutableString cleanText, ReferenceArrayList<Annotation> charPosAnnotations) throws IOException {
		int beginCharPos = -1;
		final FastBufferedReader rawFbr = new FastBufferedReader(rawText);
		while(rawFbr.readLine(line) != null) {
			lineToColumns(line, columns);
			if (columns.size() - 1 < BarcelonaCorpus.tokenCol) continue;
			tentativeToken.replace(columns.get(BarcelonaCorpus.tokenCol));
			if (tentativeToken.startsWith(metaTokenPrefix)) continue;
			final int beforeCharPos = cleanText.length();
			cleanText.append(tentativeToken);
			cleanText.append(' ');
			if (columns.size() - 1 < BarcelonaCorpus.linkCol) continue;
			rawLink.replace(columns.get(BarcelonaCorpus.linkCol));
			if (BarcelonaCorpus.isInLink(rawLink, cleanLink)) {
					// nothing, pos ticks on
			}
			else {
				// either begin link or no link, check if pending annot flush
				if (entName.length() > 0 && beginCharPos != -1) {
					final Annotation charAnnot = new Annotation(entName.toString(), Interval.valueOf(beginCharPos, beforeCharPos-2), 0, 0);
					// minus one because right endpoint is included, minus one for the added space char
					charPosAnnotations.add(charAnnot);
					entName.length(0);
					beginCharPos = -1;
				}
				if (BarcelonaCorpus.isBeginLink(rawLink, cleanLink)) {
					entName.replace(cleanLink);
					beginCharPos = beforeCharPos;
				}
			}
		}
		if (entName.length() > 0 && beginCharPos != -1) {
			final Annotation charAnnot = new Annotation(entName.toString(), Interval.valueOf(beginCharPos, cleanText.length()-1), 0, 0);
			charPosAnnotations.add(charAnnot);
		}
		rawFbr.close();
	}

	static void lineToColumns(MutableString line, ReferenceArrayList<CharSequence> columns) {
		columns.clear();
		for (int lookFrom = 0, lookSize = line.length(); lookFrom < lookSize; ) {
			final int nextColPos = line.indexOf(columnEndChar, lookFrom);
			if (nextColPos == -1) {
				if (lookFrom < lookSize) {
					columns.add(line.subSequence(lookFrom, lookSize));
				}
				break;
			}
			columns.add(line.subSequence(lookFrom, nextColPos));
			lookFrom = nextColPos + 1;
		}
	}
	
	/* IDocument methods */

	@Override
	public int docidAsInt() {
		if (docId > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Document ID " + docId + " too large for int.");
		}
		return (int) docId;
	}

	@Override
	public long docidAsLong() {
		return docId;
	}

	@Override
	public boolean nextWordToken(MutableInt outOffset, MutableString outWordText) {
		if (wnwIndex > wordsNonWordsBeginCharPosInCleanText.size() - 2) {
			return false;
		}
		if (wnwIndex % 2 != 0) {
			throw new IllegalStateException("Word/non-word array has odd size.");
		}
		outOffset.setValue(wnwIndex/2);
		final int wordBeginCharPos = wordsNonWordsBeginCharPosInCleanText.getInt(wnwIndex);
		final int wordEndCharPos = wordsNonWordsBeginCharPosInCleanText.getInt(wnwIndex + 1);
		outWordText.replace(cleanText.subSequence(wordBeginCharPos, wordEndCharPos));
		wnwIndex += 2;
		return true;
	}
	
	@Override
	public boolean reset() {
		if (wordsNonWordsBeginCharPosInCleanText.size() % 2 != 0) {
			throw new IllegalStateException("Word/non-word array has odd size.");
		}
		wnwIndex = 0;
		return true;
	}
	
	@Override
	public int numWordTokens() {
		return wordsNonWordsBeginCharPosInCleanText.size() / 2;
	}
	
	@Override
	public CharSequence wordTokenAt(int wordPos) {
		final int wxPos = wordPos * 2;
		final int wordBeginCharPos = wordsNonWordsBeginCharPosInCleanText.getInt(wxPos);
		final int wordEndCharPos = wordsNonWordsBeginCharPosInCleanText.getInt(wxPos + 1);
		return cleanText.subSequence(wordBeginCharPos, wordEndCharPos);
	}
	
	@Override
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase) {
		phrase.clear();
		for (int cx = span.left; cx <= span.right; ++cx) {
			phrase.add(wordTokenAt(cx));
		}
	}
	
	public Collection<Annotation> getReferenceAnnotations() {
		return referenceAnnotations;
	}
}
