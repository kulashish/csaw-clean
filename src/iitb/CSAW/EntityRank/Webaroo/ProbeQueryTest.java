package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TDoubleIntHashMap;
import gnu.trove.TDoubleIntIterator;
import gnu.trove.TIntDoubleIterator;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.Webaroo.WebarooStripeManager;
import iitb.CSAW.EntityRank.PropertyKeys;
import iitb.CSAW.EntityRank.SnippetFeatureVector;
import iitb.CSAW.EntityRank.Feature.AFeature;
import iitb.CSAW.EntityRank.Feature.IdfXProximityGridCell;
import iitb.CSAW.EntityRank.InexTrec.QueryWithAnswers;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.TokenLiteralWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Search.ProbeQueryProcessor;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.Sort.BitExternalMergeSort;
import iitb.CSAW.Utils.Sort.BitSortedRunWriter;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.OutputBitStream;

import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import cern.colt.list.DoubleArrayList;

import com.jamonapi.MonitorFactory;

/**
 * Badly named. Dumps {@link SnippetFeatureVector}s to disk 
 * in response to queries.
 * @author soumen
 */
public class ProbeQueryTest {
	
	public static double eta = 1.0E-4; // some small value
	
	/**
	 * @param args [0]=props [1]=log [2]=opcode
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		ProbeQueryTest pqt = new ProbeQueryTest(conf);
		if (args[2].equals("generate")) {
			// Run on all index stripes
			pqt.main2();
		}
		else if (args[2].equals("merge")) {
			// Wait for all generate tasks to end, then
			// merge on one host
			pqt.main3();
		}
	}
	
	final Logger logger = Logger.getLogger(ProbeQueryTest.class);
	final Config conf;
	
	ProbeQueryTest(Config conf) {
		this.conf = conf;
	}
		
	@SuppressWarnings("unchecked")
	void main2() throws Exception {
		final AStripeManager stripeManager = new WebarooStripeManager(conf);
		final File snippetDir = new File(conf.getString(PropertyKeys.snippetDirKey));
		assert snippetDir.isDirectory();
		final File snippetFile = new File(snippetDir, PropertyKeys.snippetFeatureVectorPrefix + "_" + stripeManager.myHostName() + PropertyKeys.snippetFeatureVectorSuffix);
		final OutputBitStream snippetObs = new OutputBitStream(snippetFile);
		boolean wantTokenFeatures = false;
		final ArrayList<AFeature> highLevelFeatures = new ArrayList<AFeature>();
		List<Object> featureNames = conf.getList(iitb.CSAW.EntityRank.PropertyKeys.snippetHighLevelFeaturesKey);
		for (String featureName : (List<String>)(Object)featureNames) {
			if(featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityRectangleCell.class.getSimpleName()) || 
					featureName.contains(iitb.CSAW.EntityRank.Feature.IdfXProximityGridCell.class.getSimpleName())){
				// just add a placeholder feature for parameterized grid/rectangle feature, with dummy arguments
				String[] parts = featureName.split("\\(");
				String parameterizedfeatureName = parts[0];
				final int dummyValue = 0;
				highLevelFeatures.add((AFeature) Class.forName(parameterizedfeatureName).getConstructor(Config.class, int.class, int.class).
						newInstance(conf,dummyValue,dummyValue));
				wantTokenFeatures = true;
			}else{
				highLevelFeatures.add((AFeature) Class.forName(featureName).getConstructor(Config.class).newInstance(conf));
			}
		}
		
		// Read number of partitions
		final int positionSlots = Integer.parseInt(conf.getString(PropertyKeys.proximitySlots)); 
		final int valueSlots = Integer.parseInt(conf.getString(PropertyKeys.idfSlots));
		
		final ACatalog catalog = ACatalog.construct(conf);
		HashMap<String, QueryWithAnswers> qwas = new HashMap<String, QueryWithAnswers>();
		QueryWithAnswers.load(conf, qwas);
		final ProbeQueryProcessor pqp = new ProbeQueryProcessor(conf);
		final ReferenceArrayList<ContextWitness> cwitsProbe = new ReferenceArrayList<ContextWitness>();
		
		long t0 = System.currentTimeMillis(), nQueries = 0;
		for (final QueryWithAnswers qwa: qwas.values()) {
			if (qwa.posEntNames.isEmpty()) {
				continue;
			}
			
//			final int atypeCatID = catalog.catNameToCatID(qwa.atypeCatName);
//			if (atypeCatID == -1) {
//				logger.warn(query.queryId + " has no recognized atype [" + qwa.atypeCatName + "]");
//				continue;
//			}
//			TIntHashSet candidteEntIDs = new TIntHashSet();
//			catalog.entsReachableFromCat(atypeCatID, candidteEntIDs);
//			logger.info(qwa.queryID + " atype " + qwa.atypeCatName + " reaches " + candidteEntIDs.size() + " ents");
//			boolean hasCatPath = false;
//			for (String posEntName : qwa.posEntNames) {
//				final int posEntID = catalog.entNameToEntID(posEntName);
//				if (posEntID == -1) {
//					logger.warn(qwa.queryID + " has unknown posEnt [" + posEntName + "]");
//				}
//				else {
//					if (candidteEntIDs.contains(posEntID)) {
//						hasCatPath = true;
//					}
//				}
//			}
//			MonitorFactory.add("hasCatPath", null, hasCatPath? 1 : 0);
			
			logger.info("executing " + qwa.rootQuery);
			try {
				pqp.execute(qwa.rootQuery, cwitsProbe);
			}
			catch (IllegalArgumentException ex1) {
				logger.error("Query " + qwa.rootQuery.queryId + " failed", ex1);
				continue;
			}
			logger.info(cwitsProbe.size() + " context witnesses");
			MonitorFactory.add("nonEmptyResult", null, cwitsProbe.isEmpty()? 0 : 1);
			MonitorFactory.add("witnessesPerQuery", null, cwitsProbe.size());

			final TIntIntHashMap docIdToNumSnippets = new TIntIntHashMap();
			final TIntIntHashMap entIdToNumMentions = new TIntIntHashMap();
			final ReferenceArrayList<TypeBindingWitness> tbws = new ReferenceArrayList<TypeBindingWitness>();
			final ReferenceArrayList<AWitness> mws = new ReferenceArrayList<AWitness>();
			boolean somePosEntFound = false;
			double[] valueDepthThresholds = new double[valueSlots];

			if(cwitsProbe.size() == 0){
				++nQueries;
				continue;
			}

			// NOTE : Query-wise normalization
			double maxval = 0; double minval = Double.MAX_VALUE;
			TDoubleIntHashMap valCount = new TDoubleIntHashMap(); int totalValCount=0;

			// no need for this loop if we do not want token level (i.e. grid/rectangle etc) features
			if(wantTokenFeatures){	

				// get the token level IDF values for normalization
				for (ContextWitness cwit : cwitsProbe) {
					/*
					 * There should be only one type binding witness. Get its entity ID and name.
					 */
					cwit.siftWitnesses(tbws, mws);
					assert tbws.size() == 1;
					final TypeBindingWitness tbw = tbws.get(0);

					final String entName = catalog.entIDToEntName(tbw.entLongId);
					final boolean entLabel = qwa.posEntNames.contains(entName);
					somePosEntFound = somePosEntFound || entLabel;

					docIdToNumSnippets.adjustOrPutValue(cwit.docId, 1, 1);
					for (AWitness aw : cwit.witnesses) {
						if (aw instanceof TypeBindingWitness) {
							final TypeBindingWitness tbw2 = (TypeBindingWitness) aw;
							entIdToNumMentions.adjustOrPutValue(tbw2.entLongId, 1, 1);
						}
					}

					// get token level idf and proximity values
					DoubleArrayList positions = new DoubleArrayList();
					DoubleArrayList values = new DoubleArrayList();
					for (int fx = 0; fx < highLevelFeatures.size(); ++fx) {
						// if it is IdfProximityFeature, then we get positions or values information
						if(highLevelFeatures.get(fx).getClass().getSimpleName().contains(iitb.CSAW.EntityRank.Feature.IdfProximityFeature.class.getSimpleName())){
							highLevelFeatures.get(fx).value3(cwit, positions, values);
						}
					}

					// update min and max (used for normalization)
					if(positions.size() > 0){ 
						for(int i = 0; i < positions.size(); i++){
							double val = values.get(i);
							if(val < minval){minval = val;}
							if(val > maxval){maxval = val;}
							valCount.adjustOrPutValue(val, 1, 1);
							totalValCount++;
						}
					}
				}


				//Get thresholds for intervals using above values
				for(int i = 0; i < valueDepthThresholds.length; i++){
					valueDepthThresholds[i] = 0;
				}
				createPartitions(valCount, totalValCount, valueDepthThresholds);
			}


			// Given the partitions, now find the histogram for each snippet (proximity v/s idf)
			for (ContextWitness cwit : cwitsProbe) {
				/*
				 * There should be only one type binding witness. Get its entity ID and name.
				 */
				cwit.siftWitnesses(tbws, mws);
				assert tbws.size() == 1;
				final TypeBindingWitness tbw = tbws.get(0);

				final String entName = catalog.entIDToEntName(tbw.entLongId);
				final boolean entLabel = qwa.posEntNames.contains(entName);

				SnippetFeatureVector sfv = new SnippetFeatureVector();
				sfv.construct(qwa.rootQuery.queryId, tbw.entLongId, cwit.docId, cwit.interval, entLabel);

				DoubleArrayList positions = new DoubleArrayList();
				DoubleArrayList values = new DoubleArrayList();
				
				// For grid features 
				int[][] grid = new int[positionSlots][valueSlots];
				int[][] rectangle = new int[positionSlots][valueSlots];
				int featureCounter = 0;
				
				for (int fx = 0; fx < highLevelFeatures.size(); ++fx) {

					String featureName = highLevelFeatures.get(fx).getClass().getSimpleName();			

					// get the constituent values
					double fv = 0;
					
					// add IdfSum features
					if(featureName.contains(iitb.CSAW.EntityRank.Feature.IdfSumFeature.class.getSimpleName())){
						fv = highLevelFeatures.get(fx).value2(cwit);
						sfv.put(featureCounter, (float) fv);
						featureCounter++;
					}
					
					// for IDFproximity feature
					if(featureName.equals(iitb.CSAW.EntityRank.Feature.IdfProximityFeature.class.getSimpleName())){

						if(!wantTokenFeatures){
							fv = highLevelFeatures.get(fx).value2(cwit);
						}else{
							fv = highLevelFeatures.get(fx).value3(cwit, positions, values);
							double posChunk = 1.0*Integer.parseInt(conf.getString(PropertyKeys.windowKey))/positionSlots + eta; // adding small eta to take care of rounding errors

							for(int i = 0; i < positions.size(); i++){
								// find position index in the histogram for ith word
								final Double pos = positions.get(i);
								double slot = 1; int normPos = 0;
								while(slot <= pos){
									slot = slot + posChunk;
									normPos += 1;
								}

								// find value index in the histogram for ith word
								final double val = values.get(i);
								slot = minval; int normVal = 1; 
								int index = 0;
								while(valueDepthThresholds[index] < val){
									index++;
									normVal += 1;
								}

								// normPod and normVal are measured 1 to max
								grid[normPos-1][normVal-1]++;

								// just to emphasize that we have low to high IDF (and near to farther proximity) bucket order 
								final int lowIdfEnd = 0; final int fartherProxEnd = positionSlots-1;
								
								// set the influence for the rectangular grid
								for(int px = normPos-1; px <= fartherProxEnd; px++){
									for(int ix = lowIdfEnd; ix <= normVal-1; ix++){
										rectangle[px][ix]++;
									}
								}
								
							}
						}
						
						sfv.put(featureCounter, (float) fv);
						featureCounter++;
					}
					
					// add grid features
					if(featureName.equals(iitb.CSAW.EntityRank.Feature.IdfXProximityGridCell.class.getSimpleName())){	
						for(int px = 0; px < positionSlots; ++px){
							for(int vx = 0; vx < valueSlots; ++vx){
								sfv.put(featureCounter, grid[px][vx]);
								++featureCounter;
							}
						}
					}
					
					// add rectangle features
					if(featureName.equals(iitb.CSAW.EntityRank.Feature.IdfXProximityRectangleCell.class.getSimpleName())){	
						for(int px = 0; px < positionSlots; ++px){
							for(int vx = 0; vx < valueSlots; ++vx){
								sfv.put(featureCounter, rectangle[px][vx]);
								++featureCounter;
							}
						}
					}
				}
				
				sfv.store(snippetObs);
			}

			MonitorFactory.add("somePosEntFound", null, somePosEntFound? 1 : 0);
			MonitorFactory.add("docsPerQuery", null, docIdToNumSnippets.size());
			MonitorFactory.add("entsPerQuery", null, entIdToNumMentions.size());

			entIdToNumMentions.forEachEntry(new TIntIntProcedure() {
				@Override
				public boolean execute(int entId, int entMult) {
					final String entName = catalog.entIDToEntName(entId);
					final boolean entLabel = qwa.posEntNames.contains(entName);
					final long entCorpusFreq = pqp.sipIndexReader.entGlobalCf(entId, false);
					final long entDocFreq = pqp.sipIndexReader.entGlobalDf(entId, false);
					logger.trace("entity " + qwa.rootQuery.queryId + " " + entName + " " + entLabel + " " + entMult + " " + entCorpusFreq + " " + entDocFreq);
					return true;
				}
			});

			++nQueries;
		}
		snippetObs.close();
		final long t1 = System.currentTimeMillis();
		logger.info(nQueries + " queries in " + (t1-t0) + "ms at " + 1d * (t1-t0) / nQueries + " ms/q");
		logger.info(MonitorFactory.getMonitor("hasCatPath", null));
		logger.info(MonitorFactory.getMonitor("somePosEntFound", null));
		logger.info(MonitorFactory.getMonitor("wholeDocMustMatch", null));
		logger.info(MonitorFactory.getMonitor("nonEmptyResult", null));
		logger.info(MonitorFactory.getMonitor("witnessesPerQuery", null));
		logger.info(MonitorFactory.getMonitor("docsPerQuery", null));
		logger.info(MonitorFactory.getMonitor("entsPerQuery", null));
		logger.info(MonitorFactory.getMonitor(TokenLiteralWitness.class.getSimpleName(), null));
		logger.info(MonitorFactory.getMonitor(EntityLiteralWitness.class.getSimpleName(), null));
		logger.info(MonitorFactory.getMonitor(TypeBindingWitness.class.getSimpleName(), null));
	}

	/**
	 * Merges and sorts sfv files from all stripes.
	 * @throws Exception
	 */
	void main3() throws Exception {
		final AStripeManager stripeManager = new WebarooStripeManager(conf);
		final File snippetDir = new File(conf.getString(PropertyKeys.snippetDirKey));
		final File sfvFile = new File(snippetDir, PropertyKeys.snippetFeatureVectorPrefix + PropertyKeys.snippetFeatureVectorSuffix);
		final File tmpDir = stripeManager.getTmpDir(stripeManager.myHostStripe());
		final BitSortedRunWriter<SnippetFeatureVector> bsrw = new BitSortedRunWriter<SnippetFeatureVector>(SnippetFeatureVector.class, 500*(1<<20));
		final ArrayList<File> sfvRunFiles = bsrw.writeRuns(tmpDir, sfvFile);
		BitExternalMergeSort<SnippetFeatureVector> sfvSorter = new BitExternalMergeSort<SnippetFeatureVector>(SnippetFeatureVector.class, tmpDir);
		sfvSorter.mergeUsingHeapLimitedFanIn(sfvRunFiles, sfvFile);
		for (File sfvRunFile : sfvRunFiles) {
			sfvRunFile.delete();
		}
	}
	
	/**
	 * Create value based thresholds for creating equi-depth histograms
	 * @param valCount
	 * @param totalValCount
	 * @param valueDepthThresholds
	 */
	public void createPartitions(TDoubleIntHashMap valCount, int totalValCount, double[] valueDepthThresholds){		
		// Create non-uniform, depth-wise partitions
		final int valueSlots = 5; // Can read this from property files
		ArrayList<Double> distinctVals = new ArrayList<Double>(); // collect all distinct values for IDF taken by constituent words/phrases

		// find distinct values and sort them
		for(TDoubleIntIterator itx = valCount.iterator(); itx.hasNext();){
			itx.advance();
			distinctVals.add(itx.key());
		}
		Collections.sort(distinctVals);

		if(distinctVals.size() > valueSlots){
			// Too many distinct values, remove least strength values

			double[] valueCountsFraction = new double[distinctVals.size()];
			for(int i = 0; i < distinctVals.size(); i++){
				valueCountsFraction[i] = 1.0 * valCount.get(distinctVals.get(i))/totalValCount;
			}

			valueDepthThresholds[0] = distinctVals.get(0) + eta;
			valueDepthThresholds[valueSlots-1] = distinctVals.get(distinctVals.size()-1) + eta;

			// assign max values to borders so that they are not removed while choosing least freq
			valueCountsFraction[0] = valueCountsFraction[distinctVals.size()-1] = Double.MAX_VALUE;  
			double[] copyOfValueCountsFraction = Arrays.copyOf(valueCountsFraction, valueCountsFraction.length);
			Arrays.sort(copyOfValueCountsFraction);
			final int toRemove = distinctVals.size() - valueSlots;
			final double cutoff = copyOfValueCountsFraction[toRemove];

			// Special case: if there are multiple values having same counts, and cutoff = that value. 
			//So we have to keep track of how many numbers having that value have to be removed
			int duplicatesToRemove = 0;
			for(int i = 0; i < toRemove; i++){
				if(cutoff == copyOfValueCountsFraction[i]){
					duplicatesToRemove++;
				}
			}

			// Remove (i.e merge with adjacent slot) thresholds having freq less than cutoff
			int j = 1; 
			for(int i = 1; i < distinctVals.size()-1; i++){
				if(valueCountsFraction[i] < cutoff){
					continue;
				}else{
					if(valueCountsFraction[i] == cutoff && duplicatesToRemove > 0){
						duplicatesToRemove--;
						continue;
					}
				}
				valueDepthThresholds[j] = distinctVals.get(i) + eta;
				j++;
			}
		}else{
			// Less distinct values than slots, need to decide position of intervals

			double[] valueCountsCumulativeFraction = new double[distinctVals.size()];

			// Need to separate partitions 
			for(int i = 0; i < distinctVals.size(); i++){
				valueDepthThresholds[i] = distinctVals.get(i);		
			}

			// Create cumulative fractions for different IDF values
			double prevValue = 0;
			for(int i = 0; i < distinctVals.size(); i++){
				valueCountsCumulativeFraction[i] = 1.0 * valCount.get(distinctVals.get(i))/totalValCount + prevValue;
				prevValue = valueCountsCumulativeFraction[i]; 
			}


			// Now shift the thresholds till they fall in proper slots
			int prevIndex = valueDepthThresholds.length;
			for(int i = distinctVals.size() - 1 ; i >= 0;  i--){   
				int j = i;
				while(j+1 < prevIndex){
					if(valueCountsCumulativeFraction[i] > ((j+1)*0.1)){
						valueDepthThresholds[j+1] = distinctVals.get(i) + eta; //adding eta to take care of rounding errors
						j++;
					}else{
						break;
					}
				}

				// Remember the position till which we shifted this value 
				int remember = j;
				while(j+1 < prevIndex){
					if(i == distinctVals.size()-1){
						break;
					}
					valueDepthThresholds[j+1] = distinctVals.get(i+1) - eta; // make sure that slots in between are not claimed by anyone 
					j++;					
				}
				prevIndex = remember;
			}
			valueDepthThresholds[0] = valueDepthThresholds[0]+eta; // add eta to the smallest value to take care of rounding errors 
		}
	}
	
}
