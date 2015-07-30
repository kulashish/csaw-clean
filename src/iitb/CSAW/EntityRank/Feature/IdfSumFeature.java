package iitb.CSAW.EntityRank.Feature;

import iitb.CSAW.EntityRank.Wikipedia.Snippet;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.PhraseWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.NotImplementedException;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;

/**
 * Sum of IDF of matched selector tokens.
 * Uses the linear form of IDF: number of documents in corpus divided by
 * the number of documents having a token. More commonly, the log of this 
 * quantity (possibly plus one) is used.
 * @author soumen
 */
public class IdfSumFeature extends AFeature {
	public IdfSumFeature(Config conf) throws IOException, ClassNotFoundException, ConfigurationException, SecurityException, URISyntaxException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		super(conf);
	}
	
	/**
	 * Ignore {@link TypeBindingWitness}es. Assume no {@link Exist#not} matchers.
	 * Locate all must and may witnesses and add up their IDFs. In case 
	 * of {@link PhraseWitness}es, use IDFs of component tokens. Also handle
	 * {@link EntityLiteralWitness}es. Because we do not pay attention to
	 * proximity yet, we do not need to know how many {@link TypeBindingWitness}es
	 * are there, or where they are.  
	 */
	@Override
	public double value2(ContextWitness cw) {
		double ans = 0;
		for (AWitness aw : cw.witnesses) {
			if (aw instanceof PhraseWitness) {
				final PhraseWitness pw = (PhraseWitness) aw;
				for (AWitness a2w : pw.atomWitnesses) {
					ans += a2w.energy();
				}
			}
			else {
				ans += aw.energy();
			}
		}
		final ContextQuery cq = (ContextQuery) cw.queryNode;
		final double output = ans / cq.energy();
		return output;
	}

	@Override
	public double value(RootQuery query, Snippet snippet) {
		final double denom = query.sumTokenIdf(); 
		double ans = 0;
		for (int matchOffset : snippet.matchOffsets) {
			final int leftGap = snippet.entBeginOffset - matchOffset;
			if (0 < leftGap && leftGap < snippet.leftStems.size()) {
				final MutableString stem = snippet.leftStems.get(leftGap);
				final TokenLiteralQuery ctl = query.findLiteral(stem);
				ans += (double) ctl.nDocs / (double) ctl.documentFreq;
			}
			final int rightGap = matchOffset - snippet.entEndOffset;
			if (0 < rightGap && rightGap < snippet.rightStems.size()) {
				final MutableString stem = snippet.rightStems.get(rightGap);
				final TokenLiteralQuery ctl = query.findLiteral(stem);
				ans += (double) ctl.nDocs / (double) ctl.documentFreq;
			}
		}
		final double output = ans / denom;
		return output;
	}

	@Override
	public double value3(ContextWitness cw, DoubleArrayList positions, DoubleArrayList values) {
		throw new NotImplementedException();
	}
}
