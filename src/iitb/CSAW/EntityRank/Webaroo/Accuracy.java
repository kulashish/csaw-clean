package iitb.CSAW.EntityRank.Webaroo;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.text.DecimalFormat;

import cern.colt.matrix.DoubleFactory2D;
import cern.colt.matrix.DoubleMatrix2D;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.DomDriver;

@SuppressWarnings("serial")
public class Accuracy implements Serializable {
	
	static final DecimalFormat df = new DecimalFormat(".000");
	final String name;
	final int maxTrainClip1;
	
	/** 
	 * Max accuracy per query, per clipRank, across C
	 */
	DoubleMatrix2D maxAccuracyAcrossC;
	
	/**
	 * C for which maxAccuracy was obtained 
	 */
	DoubleMatrix2D argMaxSvmC;
	
	/**
	 * Per cliprank, gives average of maxAccuracy across all queries
	 */
	double[] averageMaxAccuracy;
	
	public Accuracy(String name, int maxTrainClip1, int numQ, double defaultVal) {
		this.name = name;
		this.maxTrainClip1 = maxTrainClip1;
		maxAccuracyAcrossC = DoubleFactory2D.dense.make(numQ, maxTrainClip1, defaultVal);
		argMaxSvmC = DoubleFactory2D.dense.make(numQ, maxTrainClip1, Double.NaN);
		averageMaxAccuracy = null;
	}
	
	public void update(int clipRank1, double svmC, double newAccuracy, int qid) {
		final int clipRank0 = clipRank1 - 1;
		final double oldAccuracy = maxAccuracyAcrossC.get(qid,clipRank0);
		if (newAccuracy > oldAccuracy) {
			maxAccuracyAcrossC.set(qid, clipRank0, newAccuracy);
			argMaxSvmC.set(qid, clipRank0, svmC);
		}
	}
	
	/**
	 * Updated code. Check for entries having default value and disregard them. Scale by #(non-default value queries)
	 */
	public void average() {
		final double colSumsMaxAcc[] = new double[maxAccuracyAcrossC.columns()];
		for (int clipRank = 0; clipRank < maxAccuracyAcrossC.columns(); ++clipRank) {
			int validQ = 0;
			for (int qid = 0; qid < maxAccuracyAcrossC.rows(); qid++){
				if(maxAccuracyAcrossC.get(qid, clipRank) != (-Double.MAX_VALUE)){
					colSumsMaxAcc[clipRank] += maxAccuracyAcrossC.get(qid, clipRank);
					validQ++;
				}
			}			
//			colSumsMaxAcc[clipRank] /= maxAccuracyAcrossC.rows();
			colSumsMaxAcc[clipRank] /= validQ;
		}
		averageMaxAccuracy = colSumsMaxAcc;
	}

	public void printMaxAccuracy(PrintStream ps) {
		printMaxAccuracy(ps, "");		
	}
	
	public void printAverageAccuracy(PrintStream ps) {
		printAverageAccuracy(ps, "");		
	}
	
	/**
	 * Print average max accuracy followed by per-query max accuracy
	 * @param ps
	 * @param header
	 */
	public void printMaxAccuracy(PrintStream ps, String header) {
		if(averageMaxAccuracy == null){
			average();
		}
		
		// print header info
		ps.print(name + " " + header);
		// print the table
		for (int clipRank = 0; clipRank < maxAccuracyAcrossC.columns(); ++clipRank) {
			ps.print(df.format(averageMaxAccuracy[clipRank]) + " ");
			for (int qid = 0; qid < maxAccuracyAcrossC.rows(); qid++){
//				ps.print(df.format(maxAccuracy.get(qid, clipRank) + " "));
				ps.print(Double.toString(maxAccuracyAcrossC.get(qid, clipRank)) + " ");
			}			 
			ps.println();
		}
	}
	
	/**
	 * Print average max accuracy  
	 * @param ps
	 * @param header
	 */
	public void printAverageAccuracy(PrintStream ps, String header){
		if(averageMaxAccuracy == null){
			average();
		}
		// print header info
		ps.print(name + " " + header);
		// print the table
		for (int clipRank = 0; clipRank < maxAccuracyAcrossC.columns(); ++clipRank) {
			ps.print(df.format(averageMaxAccuracy[clipRank]) + " ");
		}
		ps.println();
	}
	
	public void printArgMaxSvmC(PrintStream ps) {
		ps.print(name + " argMaxSvmC");
		for (int clipRank = 0; clipRank < argMaxSvmC.columns(); ++clipRank) {
			for (int qid = 0; qid < argMaxSvmC.rows(); qid++){			
//				ps.print(df.format(argMaxSvmC.get(qid, clipRank) + " "));
				ps.print(Double.toString(argMaxSvmC.get(qid, clipRank)) + " ");
			}
			ps.println();
		}
		ps.print("\n\n");
	}
    
    public void printAsXML(File xmlFile) throws IOException {
        final XStream xstream = new XStream(new DomDriver());
        PrintStream xmlStream = new PrintStream(xmlFile);
        xmlStream.println("<object-stream>");
        xstream.toXML(this, xmlStream);
        xmlStream.println("</object-stream>");
        xmlStream.close();
    }
    
    public static Accuracy loadFromXML(File xmlFile) throws IOException {
        final XStream xstream = new XStream(new DomDriver());
        FileReader xmlReader = new FileReader(xmlFile); 
        Accuracy ans = (Accuracy) xstream.fromXML(xmlReader);
        xmlReader.close();
        return ans;
    }
	
	public static class All {
        final String mapName = "-map.xml", ndcgName = "-ndcg.xml", precName = "-prec.xml", mrrName = "-mrr.xml", pSwapName = "-pairSwap.xml";
		public final Accuracy mapAccuracy, ndcgAccuracy, precAccuracy, mrrAccuracy, pairSwapAccuracy;
		public All(int numQ, int maxTrainClip1) {
			pairSwapAccuracy = new Accuracy("pairSwaps", 1, numQ, -Double.MAX_VALUE);
			mapAccuracy = new Accuracy("map", 1, numQ, -Double.MAX_VALUE);
			ndcgAccuracy = new Accuracy("ndcg", maxTrainClip1, numQ, -Double.MAX_VALUE);
			precAccuracy = new Accuracy("prec", maxTrainClip1, numQ, -Double.MAX_VALUE);
			mrrAccuracy = new Accuracy("mrr", maxTrainClip1, numQ, -Double.MAX_VALUE);
		}
		
		public void update(int clipRank1, double svmC, RankEvaluator rev, int qid) {
			if(clipRank1==1){
				double t = rev.pairSwaps();
				pairSwapAccuracy.update(1, svmC, t, qid);
				mapAccuracy.update(1, svmC, rev.meanAveragePrecision(), qid);
			}
			ndcgAccuracy.update(clipRank1, svmC, rev.ndcg(clipRank1), qid);
			precAccuracy.update(clipRank1, svmC, rev.precision(clipRank1), qid);
			mrrAccuracy.update(clipRank1, svmC, rev.mrrAtPosition(clipRank1), qid);
		}
		
		/**
		 * pairswap, ndcg, precision, mrr, map, for each clipRank
		 * @param ps
		 */
		public void printText(PrintStream ps) {
			pairSwapAccuracy.printAverageAccuracy(ps);
			ndcgAccuracy.printAverageAccuracy(ps);
			precAccuracy.printAverageAccuracy(ps);
			mrrAccuracy.printAverageAccuracy(ps);
			mapAccuracy.printAverageAccuracy(ps);
		}
		
		/**
		 * pairswap, ndcg, precision, mrr, map, for each clipRank. Each row is for 1 cliprank, average followed by querywise numbers 
		 * @param ps
		 */
		public void printMaxAccuracyText(PrintStream ps) {
			pairSwapAccuracy.printMaxAccuracy(ps);
			ndcgAccuracy.printMaxAccuracy(ps);
			precAccuracy.printMaxAccuracy(ps);
			mrrAccuracy.printMaxAccuracy(ps);
			mapAccuracy.printMaxAccuracy(ps);
			
			pairSwapAccuracy.printArgMaxSvmC(ps);
			ndcgAccuracy.printArgMaxSvmC(ps);
            precAccuracy.printArgMaxSvmC(ps);
            mrrAccuracy.printArgMaxSvmC(ps);
            mapAccuracy.printArgMaxSvmC(ps);
		}
        
		public void average() {
			pairSwapAccuracy.average();
			ndcgAccuracy.average();
			precAccuracy.average();
			mrrAccuracy.average();
			mapAccuracy.average();
		}
	}
}
