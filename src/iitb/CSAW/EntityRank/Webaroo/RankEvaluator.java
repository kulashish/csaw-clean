package iitb.CSAW.EntityRank.Webaroo;

/**
 * Given a total or partial order on documents in response to a query, computes
 * various merit figures, like pairswaps, MAP, MRR, NDCG, etc. 
 * @author soumen
 * @author uma
 */
public class RankEvaluator {
	final boolean[] labels;
	final int numPosGroundTruth;

	public RankEvaluator(boolean[] labels, int numPosGroundTruth) {
		// assert labels.length > 0;
		// sc 2012/10/15 empty labels is possible
		this.labels = labels;
		this.numPosGroundTruth = numPosGroundTruth;
	}
	
	/* --- Pairswaps --- */
	
	/**
	 * Accuracy proportinalTo -pairSwaps, hence returns -pairSwaps (in fraction) 
	 */
	public double pairSwaps() {
		int rank0 = 0;
		double numGood = 0, numBad = 0, numPairSwaps = 0;
		for (rank0 = 0; rank0 < labels.length; ++rank0) {
			final boolean label = labels[rank0];
			if (label == true) {
				numPairSwaps += numBad;
				numGood++;
			}else{
				numBad++;
			}
		}
		
		double ans = 0.0; 
		if(numBad > 0 && numGood == 0){
			ans = -1.;
		}else{
			if(numGood > 0 && numBad == 0){
				ans = 0;
			}else{
				//Assume that (numPosGroundTruth - numGood) no. of positives are at the end (i.e. just after last elem of retrieved)
				double additionalPSwaps = (numPosGroundTruth - numGood) * numBad;
				ans = -1.0*(numPairSwaps + additionalPSwaps)/(numGood*numBad + additionalPSwaps);
			}
		}
		return ans;
	}
	
	/* --- MRR at position --- */
	public double mrrAtPosition(int mrrClipRank) {
		assert mrrClipRank > 0;

		int rank0 = 0;
		for (rank0 = 0; rank0 < mrrClipRank && rank0 < labels.length; ++rank0) {
			final boolean label = labels[rank0];
			if (label == true) {
				final double rrForThisQuery = 1f / (rank0 + 1);				
				return rrForThisQuery;
			}
		}		
		return 0;		
	}

	/* --- MRR trec style --- */
	public double mrr() {
		int rank0 = 0;
		for (rank0 = 0; rank0 < labels.length; ++rank0) {
			final boolean label = labels[rank0];
			if (label == true) {
				final double rrForThisQuery = 1f / (rank0 + 1);				
				return rrForThisQuery;
			}
		}		
		return 0;		
	}
	
	/* --- Precision --- */

	public double precision(int precisionClipRank) {
		assert precisionClipRank > 0;

		double numGoodAtTop = 0;

		for (int rank0 = 0; rank0 < precisionClipRank && rank0 < labels.length; ++rank0) {
			final boolean label = labels[rank0];
			numGoodAtTop += (label == true)? 1 : 0;
		}
		return (numGoodAtTop / precisionClipRank);
	}

	/* --- Mean average precision --- */
	public double meanAveragePrecision() {

		float prefixNumGood = 0, querySumPrecision = 0;
		for (int rx = 0; rx < labels.length; ++rx) {
			final boolean label = labels[rx];
			if (label == true) {
				prefixNumGood += 1;
				final float prefixPrecision = prefixNumGood / (1+rx);
				querySumPrecision += prefixPrecision;
			}
		}
		if (prefixNumGood > 0) {
//			return(querySumPrecision / prefixNumGood);
			return(querySumPrecision / numPosGroundTruth);
		}
		return(0);
	}

	/* --- NDCG for two-level relevance only --- */

	static final double ln2 = Math.log(2);

	public static double log2(double x) {
		return Math.log(x) / ln2;
	}

	public double ndcg(int ndcgClipRank) {
		assert ndcgClipRank > 0;
		double DCG = 0;
		int numGood = 0;

		for (int rank0 = 0; rank0 < ndcgClipRank && rank0 < labels.length; ++rank0) {
			final boolean label = labels[rank0];
			if (label == true) {
				DCG += discount(rank0);
				numGood++;
			}
		}
		if (DCG > 0) {
			final double perfectDCG = perfectDCG(numGood , ndcgClipRank);
			final double ndcg = DCG / perfectDCG;
			assert !Double.isNaN(ndcg) && !Double.isInfinite(ndcg);
			return(ndcg);
		}
		return(0);
	}
	
	/**
	 * Assume numGood good documents at the top.
	 * @param numGood
	 * @param horizon
	 * @return
	 */
	public static double perfectDCG(int numGood, int horizon) {
		assert horizon > 0;
		double ans = 0;
		for (int rank0 = 0; rank0 < numGood && rank0 < horizon; ++rank0) {
			ans += discount(rank0);
		}
		return ans;
	}

	/**
	 * @param rank0 rank starting from zero
	 * @return discount to be <em>multiplied</em> with gain. Note, no clipping.
	 */
	public static double discount(int rank0) {
		//		return 1d / log2(2 + rank0);
		final int rank1 = rank0 + 1;
		assert rank1 >= 0;
		if (rank1 <= 2) return 1;
		return 1d / log2(rank1);
	}

}
