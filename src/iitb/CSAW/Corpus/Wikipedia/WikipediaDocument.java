package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.IAnnotatedDocument;
import iitb.CSAW.Index.Annotation;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;

/**
 * Everything is mutable to reuse RAM and make operations fast.
 * Requires concomitant care! A document is a {@link CharSequence}.
 * A suitable tokenizer chops the document into char segments. The tokenizer
 * used will in general depend on the field desired. Each char segment is a
 * <em>word</em> (i.e., counts in indexing) token or a <em>non-word</em>
 * (skipped during indexing) token. These strictly interleave and are
 * each maximal (as in MG4J).
 * @author soumen
 */
@Deprecated
public class WikipediaDocument implements IAnnotatedDocument {
	@Override public int docidAsInt() { return (int) docId; }
	@Override public long docidAsLong() { return docId; }
	
	@Override public boolean  reset() {
		tokenCursor = 0;
		return true;
	}
	
	@Override public boolean nextWordToken(MutableInt outOffset, MutableString outWordText) {
		if (tokenCursor >= wordTokenOffset.size()) {
			return false;
		}
		outOffset.setValue(wordTokenOffset.getInt(tokenCursor));
		outWordText.replace(textBuffer.subSequence(wordTokenBeginAtChar.getInt(tokenCursor), wordTokenEndAtChar.getInt(tokenCursor)));
		++tokenCursor;
		return true;
	}
	
	public void clear() {
		tokenCursor = 0;
		textBuffer.length(0);
		wordTokenOffset.clear();
		wordTokenBeginAtChar.clear();
		wordTokenEndAtChar.clear();
	}
	
	public long docId;
	public final MutableString title = new MutableString();
	public final MutableString url = new MutableString();
	int tokenCursor = 0;
	
	/**
	 * Original text minus markups but with case, space and punctuation preserved.
	 */ 
	public final MutableString textBuffer = new MutableString();
	/**
	 * Considering only word tokens
	 */
	public final IntArrayList wordTokenOffset = new IntArrayList();
	public final IntArrayList wordTokenBeginAtChar = new IntArrayList();
	public final IntArrayList wordTokenEndAtChar = new IntArrayList();
	
	@Override
	public String toString() {
		MutableString ans = new MutableString();
		for (int ix = 0; ix < wordTokenOffset.size(); ++ix) {
			ans.append(String.format("%6d %s\n", wordTokenOffset.getInt(ix), textBuffer.subSequence(wordTokenBeginAtChar.getInt(ix), wordTokenEndAtChar.getInt(ix))));
		}
		return ans.toString();
	}
	
	/* 
	 * The following method stubs were added after we moved off entity and type
	 * span indexing from MG4J to our SIP framework.
	 */
	
	@Override
	public int numWordTokens() {
		throw new NotImplementedException();
	}
	
	@Override
	public CharSequence wordTokenAt(int wordPos) {
		throw new NotImplementedException();
	}
	
	@Override
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase) {
		throw new NotImplementedException();
	}
	@Override
	public Collection<Annotation> getReferenceAnnotations() {
		throw new NotImplementedException();
	}
}
