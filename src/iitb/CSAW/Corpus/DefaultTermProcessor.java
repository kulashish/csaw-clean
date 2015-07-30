package iitb.CSAW.Corpus;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.TermProcessor;
import it.unimi.dsi.mg4j.index.snowball.EnglishStemmer;

@SuppressWarnings("serial")
public class DefaultTermProcessor extends EnglishStemmer {
	public static final int MAX_TERM_LENGTH = 64;
	
	@Override
	public boolean processTerm(MutableString term) {
		if (term.length() > MAX_TERM_LENGTH) {
			return false;
		}
		term.trim().toLowerCase();
		final boolean rc = super.processTerm(term);
		if (term.length() == 0) {
			return false;
		}
		return rc;
	}
	
	/**
	 * Already returns a clone, no need to {@link #copy()}.
	 * @param conf
	 * @return a cloned {@link TermProcessor}
	 */
	public static TermProcessor construct(Config conf) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		return ((TermProcessor) Class.forName(conf.getString(iitb.CSAW.Index.PropertyKeys.termProcessorName)).newInstance()).copy();
	}
}
