package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Spotter.MentionRecord;
import it.unimi.dsi.lang.MutableString;

import java.util.List;

/**
 * Writes out a sequence of {@link CharSequence} tokens into one {@link String}
 * so that it can be used as a hash key. Separated from {@link MentionCollector}.
 * @author soumen
 */
public class PhraseWriter {
	final char tokenSep = '|';
	final String tokenSepRegex = "\\|";
	final MutableString buf = new MutableString();
	
	public void makeArray(String phrase, List<String> array) {
		array.clear();
		for (String tok : phrase.split(tokenSepRegex)) {
			array.add(tok);
		}
	}
	
	public String makePhrase(List<? extends CharSequence> phrase) {
		buf.length(0);
		for (CharSequence word : phrase) {
			checkDelim(word);
			if (buf.length() > 0) {
				buf.append(tokenSep);
			}
			buf.append(word == null? "" : word);
		}
		return buf.toString();
	}
	
	public String makePhrase(MentionRecord mention) {
		buf.length(0);
		for (int tx = 0, tn = mention.size(); tx < tn; ++tx) {
			if (buf.length() > 0) {
				buf.append(tokenSep);
			}
			final MutableString atok = mention.token(tx); 
			checkDelim(atok);
			buf.append(atok == null? "" : atok);
		}
		return buf.toString();
	}

	private void checkDelim(CharSequence token) {
		if (token == null) return;
		for (int cx = 0, cn = token.length(); cx < cn; ++cx) {
			if (tokenSep == token.charAt(cx)) {
				throw new IllegalArgumentException("Bad token _" + token + "_");
			}
		}
	}
}