package iitb.CSAW.EntityRank.Wikipedia;

import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.index.NullTermProcessor;
import it.unimi.dsi.mg4j.index.TermProcessor;

public class SelectorMatcher {
	MutableString stem = new MutableString();
	final String[] phrase;
	int pos = 0;
	/**
	 * @param selector either a single token or a "quoted phrase", but no
	 * prefix + or {entity} allowed.
	 * @param termProcessor to transform each selector token as has been done in the corpus
	 */
	public SelectorMatcher(String selector, TermProcessor termProcessor) {
		final TermProcessor termProcessorCopy = termProcessor.copy();
		if (selector.length() > 0 && (selector.startsWith("+") || selector.startsWith("{"))) {
			throw new IllegalArgumentException("No +sel or {ent} allowed");
		}
		if (selector.length() > 2 && selector.startsWith("\"") && selector.endsWith("\"")) {
			selector = selector.substring(1, selector.length() - 1);
		}
		phrase = selector.split("\\s+");
		for (int px = 0; px < phrase.length; ++px) {
			stem.replace(phrase[px]);
			termProcessorCopy.processTerm(stem);
			phrase[px] = stem.toString();
		}
		pos = 0;
	}
	
	public int size() {
		return phrase.length;
	}
	
	public boolean match(MutableString token) {
		if (token.equals(phrase[pos])) {
			++pos;
		}
		else {
			pos = 0;
		}
		if (pos == phrase.length) {
			pos = 0;
			return true;
		}
		else {
			return false;
		}
	}
	
	public boolean match(String token) {
		return match(new MutableString(token));
	}
	
	public static void main(String[] args) {
		SelectorMatcher sm = new SelectorMatcher("\"had a little\"", NullTermProcessor.getInstance());
		for (String token : "little mary had a little lamb had a mary".split("\\s+")) {
			System.out.println(sm.match(token) + "\t" + token);
		}
	}
}
