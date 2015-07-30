package iitb.CSAW.Corpus;

import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;

import iitb.CSAW.Index.Annotation;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.mg4j.document.IDocument;
import it.unimi.dsi.util.Interval;

/**
 * {@link IDocument} is meant for MG4J alone.  Here we add a method for
 * getting reference (i.e., ground truth) annotations.  We also retain a 
 * couple of legacy token scanner methods for now.
 * 
 * @author soumen
 */
public interface IAnnotatedDocument extends IDocument {
	/** Once we get beyond a billion or two documents. */
	public long docidAsLong();
	
	/**
	 * Resets word cursor to the beginning of the document.
	 * @return if reset is supported by the implementation.
	 */
	public boolean reset();
	
	/**
	 * @param outOffset Nondecreasing (but not necessarily strictly increasing)
	 * with successive calls.
	 * @param outWordText Text of the indexable word. Case preserved. In case
	 * of annotations, standardized string representations. 
	 * @return If another indexable token was found.
	 */
	public boolean nextWordToken(MutableInt outOffset, MutableString outWordText);
	
	public void wordTokensInSpan(Interval span, List<CharSequence> phrase);
	
	public Collection<Annotation> getReferenceAnnotations();
}
