package iitb.CSAW.Corpus;

import iitb.CSAW.Index.Annotation;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;

/**
 * Self-contained short document for testing.
 */
public class TestDocument implements IAnnotatedDocument {
	final long docId;
	final String[] tokens;
	final ArrayList<Annotation> annots = new ArrayList<Annotation>();
	private int cursor=0;
	
	public TestDocument(long docId, String...tokens) {
		this.docId = docId;
		this.tokens = tokens;
	}
	
	public void addAnnot(int left, int right, String entName, float score, int rank) {
		annots.add(new Annotation(entName, Interval.valueOf(left, right), score, rank));
	}

	@Override public long docidAsLong() { return docId;	}
	@Override public int docidAsInt() { return (int) docId; }
	@Override public boolean reset() { cursor = 0; return true; }
	@Override public boolean nextWordToken(MutableInt outOffset, MutableString outWordText) {
		if (cursor >= tokens.length) return false;
		outOffset.setValue(cursor);
		outWordText.replace(tokens[cursor]);
		++cursor;
		return true;
	}
	@Override public Collection<Annotation> getReferenceAnnotations() { return annots; }
	@Override public int numWordTokens() { return tokens.length; }
	@Override public CharSequence wordTokenAt(int wordPos) { return tokens[wordPos]; }

	@Override
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase) {
		phrase.clear();
		for (int pos : span) {
			phrase.add(wordTokenAt(pos));
		}
	}
}