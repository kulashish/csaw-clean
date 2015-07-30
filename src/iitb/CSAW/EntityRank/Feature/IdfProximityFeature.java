package iitb.CSAW.EntityRank.Feature;

import iitb.CSAW.EntityRank.Wikipedia.Snippet;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.PhraseWitness;
import iitb.CSAW.Index.TokenLiteralWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Query.ContextQuery;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.IQuery;
import iitb.CSAW.Query.MatcherQuery;
import iitb.CSAW.Query.PhraseQuery;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Query.TokenLiteralQuery;
import iitb.CSAW.Query.MatcherQuery.Exist;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.IntArrayList;

/**
 * IDF-weighted average of proximity to all query tokens. IDF is linear.
 * Proximity is hardwired to reciprocal of lexical distance in tokens. 
 * @author soumen
 */
public class IdfProximityFeature extends AFeature {
	public IdfProximityFeature(Config config) throws IOException, ClassNotFoundException, ConfigurationException, SecurityException, URISyntaxException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		super(config);
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
				ans += (1f / leftGap) * ctl.nDocs / ctl.documentFreq;
			}
			final int rightGap = matchOffset - snippet.entEndOffset;
			if (0 < rightGap && rightGap < snippet.rightStems.size()) {
				final MutableString stem = snippet.rightStems.get(rightGap);
				final TokenLiteralQuery ctl = query.findLiteral(stem);
				ans += (1f / rightGap) * ctl.nDocs / ctl.documentFreq;
			}
		}
		final double output = ans / denom;
		return output;
	}

	@Override
	public double value2(ContextWitness cw) {
		final ReferenceArrayList<TypeBindingWitness> tbws = new ReferenceArrayList<TypeBindingWitness>();
		final ReferenceArrayList<AWitness> mws = new ReferenceArrayList<AWitness>();
		cw.siftWitnesses(tbws, mws);
		final double queryEnergy = ((ContextQuery) cw.queryNode).energy();
		double sumTbw = 0, numTbw = 0;
		for (TypeBindingWitness tbw : tbws) {
			double numerTbwScore = 0;
			for (AWitness mw : mws) {
				final double prox = proximity(tbw, mw);
				final double gotEnergy = mw.energy();
				numerTbwScore += prox * gotEnergy;
			}
			sumTbw += numerTbwScore / queryEnergy;
			numTbw += 1;
		}
		return sumTbw / numTbw;
	}
	
	@Override
	public double value3(ContextWitness cw, DoubleArrayList positions, DoubleArrayList values) {
		final ReferenceArrayList<TypeBindingWitness> tbws = new ReferenceArrayList<TypeBindingWitness>();
		final ReferenceArrayList<AWitness> mws = new ReferenceArrayList<AWitness>();
		cw.siftWitnesses(tbws, mws);
		final double queryEnergy = ((ContextQuery) cw.queryNode).energy();
		double sumTbw = 0, numTbw = 0;
		
		for (TypeBindingWitness tbw : tbws) {
			double numerTbwScore = 0;
			for (AWitness mw : mws) {
				final double prox = proximity(tbw, mw);
				final double gotEnergy = mw.energy();
				numerTbwScore += prox * gotEnergy;
				
				// Send back positions and values only if the AWitness is of correct type
				if(mw instanceof PhraseWitness){
					final PhraseQuery pq = (PhraseQuery) mw.queryNode;
					if (pq.exist == Exist.may) {
						positions.add(1d/prox);
						values.add(gotEnergy);
					}else{
						positions.add(1d/prox);
						values.add(0);
					}
				}else if (mw instanceof TokenLiteralWitness){
						final TokenLiteralQuery tlq = (TokenLiteralQuery) mw.queryNode;
						if (tlq.exist == Exist.may) {
							positions.add(1d/prox);
							values.add(gotEnergy);
						}else{
							positions.add(1d/prox);
							values.add(0);
						}
					}else if(mw instanceof EntityLiteralWitness){
						final EntityLiteralQuery elq = (EntityLiteralQuery) mw.queryNode;
						if (elq.exist == Exist.may) {
							positions.add(1d/prox);
							values.add(gotEnergy);
						}else{
							positions.add(1d/prox);
							values.add(0);
						}
					}
				
			}
			sumTbw += numerTbwScore / queryEnergy;
			numTbw += 1;
		}
		return sumTbw / numTbw;
	}
}
