package iitb.CSAW.EntityRank.Webaroo;

import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.EntityRank.SnippetFeatureVector;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.io.InputBitStream;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.configuration.ConfigurationException;
 
/**
 * @author uma
 * 1. Read the feature vector dump and create folds
 * 2. Find stats such as #usable queries, pos & neg entities  
 */
public class SfvFoldMaker {
	/* Eliminate embedded magic strings! */
	static final String usableQueryListFileName = "/mnt/b100/d0/uma/entityRank/barcelona/usableQueryList.dat";
	static final String foldFileName = "/mnt/b100/d0/uma/entityRank/barcelona/folds.dat";
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ConfigurationException 
	 * @throws ClassNotFoundException 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws ConfigurationException, IOException, ClassNotFoundException {

		// default value = 5, override using commandline params
		int numPartitions = 5;
		if(args.length > 2){
			numPartitions = Integer.parseInt(args[2]);
		}
		
		// Read the SnippetFeatureVector file and make a list of unusable queries
//		HashSet<String> usableQueries = findUsableQueries(config, usableQueryListFileName);
		
		// Read the pre-decided list of usable queries off the file
		ObjectInputStream oq = new ObjectInputStream(new FileInputStream(usableQueryListFileName));
		HashSet<String> usableQueries = (HashSet<String>) oq.readObject();
		
		System.out.println("#Useful queries for ranking (having pos + neg entities) = "+usableQueries.size());
		
		
		// Create folds out of useful queries. Print stats for each fold 
//		HashMap<Integer, HashSet<String>> folds = createFolds(numPartitions, foldFileName, seed, usableQueries);
		
		// Read pre-computed folds
		ObjectInputStream of = new ObjectInputStream(new FileInputStream(foldFileName));
		HashMap<Integer, HashSet<String>> folds = (HashMap<Integer, HashSet<String>>) of.readObject();

		for(int i = 1; i <= numPartitions; i++){
			System.out.println("fold "+i+" size = "+folds.get(i).size());
		}
	}
	

	/** Given a pseudo random float, map it to a partition. Used to find the fold to which a query belongs */
	private static int getFoldID(float numberToMap, int numFolds){
		for(int i = 1; i < numFolds; i++){
			if(numberToMap < 1.0*i/numFolds){
				return i;
			}
		}
		return numFolds;
	}

	/** Given a list of queries, divide them in k folds
	 * The folds HashMap<Integer, HashSet<String>> i.e. foldNum, queryIds is written to a file and returned as well
	 * @param foldFileName File to which folds are written. Upto the calling method to read them and create train-test splits.
	 * @param usefulQueries : list of queries to be partitioned
	 * @return folds  
	 * */
	public static HashMap<Integer, HashSet<String>> createFolds(int numFolds, String foldFileName, int randomGenSeed, HashSet<String> usefulQueries) throws IOException{
		
		Random randomGen = new Random(randomGenSeed);
		HashMap<Integer, HashSet<String>> folds = new HashMap<Integer, HashSet<String>>(); 
		
		Iterator<String> iter = usefulQueries.iterator();
		while(iter.hasNext()){
			String qid = iter.next();
			int p = getFoldID(randomGen.nextFloat(), numFolds);
			
			// add query to appropriate fold
			if(!folds.containsKey(p)){
				HashSet<String> foldWiseQuerySet = new HashSet<String>();
				foldWiseQuerySet.add(qid);
				folds.put(p,foldWiseQuerySet);
			}
			folds.get(p).add(qid);			
		}
		
		for(int i = 1; i <= numFolds; i++){
			System.out.print("fold"+i+" size = "+folds.get(i).size()+", ");
		}
		System.out.println();
		
		// Print folds to a file, which can be read later
		ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(foldFileName));
		o.writeObject(folds);
		o.close();
		
		return folds;
	}

	/**
	 * Read the SnippetFeatureVector file and collect stats like #queries having no positive entity. Finally make a list of queries which cannot be used. 
	 * This exclude-query-list will be used by fold creator
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static HashSet<String> findUsefulQueries(Config conf, String usableQueryListFileName) throws FileNotFoundException, IOException{

		final File SFVfile = new File(conf.getString(PropertyKeys.snippetDirKey)+"/SnippetFeatureVector.dat");
		
		SnippetFeatureVector sfv = new SnippetFeatureVector();
		InputBitStream sfvIbs = new InputBitStream(SFVfile);
		long trueL = 0, falseL = 0;
		String currentQ = null;
		
		HashSet<String> onlyPosQueries = new HashSet<String>();	// Queries with only positive entities - not useful for ranking
		HashSet<String> onlyNegQueries = new HashSet<String>(); // Queries with only negative entities - not useful for ranking
		HashSet<String> usableQueries = new HashSet<String>(); // Rest of the queries
		int distinctQ = 0; // Total number of queries
		
		for (long nRec = 0;;) {
			try {
				sfv.load(sfvIbs);
				if(currentQ == null || !currentQ.equals(sfv.qid.toString())){
					// new query
					distinctQ++;
				}
				currentQ = sfv.qid.toString();
				
				// Given the label of current snippet, find the correct set to which the query should belong - onlyPos, onlyNeg or usable queries
				if(sfv.entLabel == true){
					trueL++;
					if(onlyNegQueries.contains(currentQ)){
						usableQueries.add(currentQ);
						onlyNegQueries.remove(currentQ);
					}else{
						if(!onlyPosQueries.contains(currentQ) && !usableQueries.contains(currentQ)){
							onlyPosQueries.add(currentQ);
						}
					}
				}else{
					falseL++;
					if(onlyPosQueries.contains(currentQ)){
						usableQueries.add(currentQ);
						onlyPosQueries.remove(currentQ);
					}else{
						if(!onlyNegQueries.contains(currentQ) && !usableQueries.contains(currentQ)){
							onlyNegQueries.add(currentQ);
						}
					}
				}
				++nRec;
				}
			catch (EOFException eofx) {
				System.out.println("numSnippets = "+nRec + ", trueLabel snippets = "+trueL+", falseLabel snippets = "+falseL+", total queries = "+distinctQ);
				System.out.println("#usable queries i.e. both pos and neg entities= "+usableQueries.size());
				System.out.println("#queries having no pos entity = "+onlyNegQueries.size());
				System.out.println("#queries having no neg entity = "+onlyPosQueries.size());
				
				// Print IDs of usable queries to a file, which can be read later
				ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(usableQueryListFileName));
				o.writeObject(usableQueries);
				o.close();
				break;
			}
		}
		sfvIbs.close();
		return usableQueries;
	}
	
	
}
