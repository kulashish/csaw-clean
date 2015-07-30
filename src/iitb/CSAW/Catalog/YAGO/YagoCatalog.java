package iitb.CSAW.Catalog.YAGO;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.IntToStringSet;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;

public class YagoCatalog extends ACatalog {
	public static final String yagoBaseDirPropName = "csaw.yago2.base.dir", cacheName = "cache";

	final Logger logger = Logger.getLogger(getClass());
	final Config props;
	final File yagoBaseDir, yagoCacheDir;
	final YagoFactory factory;

	private static YagoCatalog _ym2 = null;
	 
	StringIntBijection catNameToID, entNameToID;
	TIntArrayList[] catToSubCats;
	TIntArrayList[] catToSupCats;
	TIntHashSet rootCats;
	TIntArrayList[] entToCats;
	TIntArrayList[] catToEnts;
	IntToStringSet entIDToLemmas;
	HashMap<String, TIntArrayList> lemmaToEntIDs;
	IntToStringSet catIDToLemmas = null;

	public static synchronized ACatalog getInstance(Config props) throws IOException {
		if (_ym2 == null) {
			_ym2 = new YagoCatalog(props);
		}
		return _ym2;
	}
	
	protected YagoCatalog(Config props) throws IOException {
		this.props = props;
		props.setThrowExceptionOnMissing(true);
		this.factory = new YagoFactory();
		yagoBaseDir = new File(props.getString(yagoBaseDirPropName));
		yagoCacheDir = new File(yagoBaseDir, cacheName);
	}
	
	/* Public access points */
	
	@Override
	public TIntHashSet rootCats() {
		if (rootCats == null) {
			factory.constructRootCats();
		}
		return rootCats;
	}
	
	@Override
	public int catNameToCatID(String catName) {
		if(catNameToID == null) {
			factory.constructCatNameMap();
		}
		return (int) catNameToID.getLong(catName);
	}
	
	@Override
	public String catIDToCatName(int catID) {
		if(catNameToID == null) {
			factory.constructCatNameMap();
		}
		return catNameToID.intToString(catID);
	}

	@Override
	public int numCats() {
		if(catNameToID == null)  {
			factory.constructCatNameMap();
		}
		return catNameToID.size();
	}

	/**
	 * @param entName Must be in canonical form, no approximate matching
	 * @return if less than zero, name was not found
	 */
	@Override
	public int entNameToEntID(String entName) {
		if (entNameToID == null) {
			factory.constructEntNameMap();
		}
		return (int) entNameToID.getLong(entName);
	}
	
	/**
	 * @param entID no checks for legitimate range
	 * @return canonical entity name 
	 */
	@Override
	public String entIDToEntName(int entID) {
		if (entNameToID == null) {
			factory.constructEntNameMap();
		}
		return entNameToID.intToString(entID);
	}

	@Override
	public int numEnts() {
		if (entNameToID == null) {
			factory.constructEntNameMap();
		}
		return entNameToID.size();
	}
	
	@Override
	public int entIDToNumLemmas(int entID) {
		if (entIDToLemmas == null) {
			factory.constructLemmaMaps();
		}
		return entIDToLemmas.keyToNumValues(entID);
	}
	
	@Override
	public void entIDToLemma(int entID, int lemmaOfs, MutableString ans) {
		if (entIDToLemmas == null) {
			factory.constructLemmaMaps();
		}
		entIDToLemmas.keyToValue(entID, lemmaOfs, ans);
	}

	/**
	 * @param lemma lowercase, tokens space separated, no wordnet_ prefix, no _\d+ suffix
	 * @return possible entity IDs to which the lemma may refer 
	 */
	public TIntArrayList lemmaToEntIDs(String lemma) {
		if (lemmaToEntIDs == null) {
			factory.constructLemmaToEntMap();
		}
		return lemmaToEntIDs.get(lemma);
	}
	
	public int catIDToNumLemmas(int catID) {
		if (catIDToLemmas == null) {
			factory.constructLemmaMaps();
		}
		return catIDToLemmas.keyToNumValues(catID);
	}
	
	public void catIDToLemma(int catID, int lemmaOfs, MutableString ans) {
		if (catIDToLemmas == null) {
			factory.constructLemmaMaps();
		}
		catIDToLemmas.keyToValue(catID, lemmaOfs, ans);
	}
	
	@Override
	public void entsReachableFromCat(int catID, TIntHashSet entIDs) {
		if(catToEnts == null) {
			factory.getCatToEntMap();
		}
		if (catToEnts[catID] != null) {
			entIDs.addAll(catToEnts[catID].toNativeArray());
		}
		if (catToSubCats == null) {
			factory.constructSubSupCats();
		}
		if (catToSubCats[catID] != null) {
			for (int subCat : catToSubCats[catID].toNativeArray()) {
				entsReachableFromCat(subCat, entIDs);
			}
		}
	}
	
	@Override
	public void subCatsReachableFromCat(int catId, TIntHashSet catIds) {
		if (catToSubCats == null) {
			factory.constructSubSupCats();
		}
		if(catToSubCats[catId] == null) {
			return;
		}
		for (int subCat : catToSubCats[catId].toNativeArray()) {
			catIds.add(subCat);
			subCatsReachableFromCat(subCat, catIds);
		}
	}
	
	@Override
	public void catsReachableFromEnt(final int entId, final TIntHashSet catIds) {
		if (entToCats == null) {
			factory.getEntToCatMap();
		}
		if (catToSupCats == null) {
			factory.constructSubSupCats();
		}
		if (entToCats[entId] == null) {
			return;
		}
		entToCats[entId].forEach(new TIntProcedure() {
			@Override
			public boolean execute(int catId) {
//				logger.debug("\t" + entIDToEntName(entId) + " isA " + catIDToCatName(catId));
				collectSupCatIds(catId, catIds);
				return true;
			}
		});
	}
	
	void collectSupCatIds(final int catId, final TIntHashSet catIds) {
		catIds.add(catId);
		if (catToSupCats[catId] == null) {
			return;
		}
		catToSupCats[catId].forEach(new TIntProcedure() {
			@Override
			public boolean execute(int supCatId) {
//				logger.debug("\t" + catIDToCatName(catId) + " subCatOf " + catIDToCatName(supCatId));
				collectSupCatIds(supCatId, catIds);
				return true;
			}
		});
	}
	
	public boolean isEntInCatTransitive(int catID, int entID) {
		if(catToEnts == null) {
			factory.getCatToEntMap();
		}
		if (catToEnts[catID] != null) {
			if (catToEnts[catID].contains(entID)) {
				return true;
			}
		}
		if (catToSubCats == null) {
			factory.constructSubSupCats();
		}
		if (catToSubCats[catID] != null) {
			for (int subCat : catToSubCats[catID].toNativeArray()) {
				if (isEntInCatTransitive(subCat, entID)) {
					return true;
				}
			}
		}
		return false;
	}
	
	void testReachable(String catName) {
		logger.debug(catName);
		if (catNameToID == null) {
			factory.constructCatNameMap();
		}
		final int catID = (int) catNameToID.getLong(catName); 
		if (catID >= 0) {
			TIntHashSet entIDs = new TIntHashSet();
			entsReachableFromCat(catID, entIDs);
			for (int entID : entIDs.toArray()) {
				logger.debug("\t" + entNameToID.intToString(entID));
			}
		}
	}
	
	private final Pattern yagoPatterns[] = {
			Pattern.compile("^wikicategory_wordnet_(.*)_\\d+$"),
			Pattern.compile("^wikicategory_(.*)$"),
			Pattern.compile("^wordnet_(.*)_\\d+$")
	};
	
	@Override
	public String canonicalToFreeText(String canon) {
		for (Pattern yagoPattern : yagoPatterns) {
			Matcher yagoMatcher = yagoPattern.matcher(canon);
			if (yagoMatcher.matches()) {
				return yagoMatcher.group(1);
			}
		}
		return canon.trim();
	}
	
	class YagoFactory {
//		private StringIntBijection catNameToID;
//		private StringIntBijection entNameToID;
//		private TIntArrayList[] catToSubCats;
//		private TIntArrayList[] catToSupCats;
//		private TIntArrayList[] entToCats;
//		private TIntArrayList[] catToEnts;
		
		private HashSet<String> keys = new HashSet<String>();
		private HashSet<String> iValues = new HashSet<String>();
		
		String iPath = "entities" + File.separator + "WordNetLinks.txt";
		
		final String catCache = "catCache";
		final String catSubCache = "catSubCache";
		final String catSupCache = "catSupCache";
		final String subClass2Type = "subClass2Type";
		final String catMeansCache = "catMeansCache";

		final String entCache = "entCache";
		final String isaCache = "isaCache";
		final String entMeansCache = "entMeansCache";
		final String catToSubEntCache = "entSupCatCache";
		
		final String entPaths[] = {
			"entities" + File.separator + "ArticleExtractor.txt",
			"entities" + File.separator + "WordNetLinks.txt",
		};
		
		final String catPaths[] = {
			"entities" + File.separator + "WordNetLinks.txt", 
			"entities" + File.separator + "ConceptLinker.txt",
		};
		
		final String subCatPaths[] = {
			"facts" + File.separator + "subClassOf" + File.separator + "WordNetLinks.txt",
			"facts" + File.separator + "subClassOf" + File.separator + "ConceptLinker.txt",
		};

		final String isaPaths[] = {
			"facts" + File.separator + "type" + File.separator + "ArticleExtractor.txt",
			"facts" + File.separator + "type" + File.separator + "CheckedFactExtractor.txt",
			"facts" + File.separator + "type" + File.separator + "IsAExtractor.txt",
		};
		
		final String meansPaths[] = {
			"facts" + File.separator + "means" + File.separator + "ArticleExtractor.txt",
			"facts" + File.separator + "means" + File.separator + "WordNetLinks.txt",
		};
		
		synchronized void constructCatNameMap() {
			if (catNameToID != null) return;
			try {
				iValues = constructSet(subCatPaths, "wnNonLeafSet", 2);
				keys.clear();
				catNameToID = constructMap(catPaths, 0, keys, catCache, iValues, true, iPath);
			} 
			catch (IOException e){
				logger.info(e+" in construction of catNameMap");
				e.printStackTrace();
			} 
			catch (ClassNotFoundException e){
				logger.info(e+" in construction of catNameMap");
				e.printStackTrace();
			}
			catNameToID.verify();
			logger.info("loaded " +catNameToID.size() + " cats");
		}
		
		synchronized void constructRootCats() {
			if (rootCats != null) return;
			constructCatNameMap();
			constructSubSupCats();
			rootCats = new TIntHashSet();
			for (int cat = 0; cat < catToSupCats.length; ++cat) {
				if (catToSupCats[cat] == null || catToSupCats[cat].isEmpty()) {
					rootCats.add(cat);
				}
			}
			logger.info("Found " + rootCats.size() + " root cats.");
		}

		synchronized void constructEntNameMap() {
			if (entNameToID != null) return;
			try {
				iValues = constructSet(subCatPaths, "wnNonLeafSet",2);
				keys.clear();
				entNameToID = constructMap(entPaths, 0, keys, entCache, iValues, false, iPath);
			} 
			catch (IOException e){
				logger.info(e+" in construction of entNameMap");
				e.printStackTrace();
			} 
			catch (ClassNotFoundException e){
				logger.info(e+" in construction of entNameMap");
				e.printStackTrace();
			}
			entNameToID.verify();
			logger.info("loaded " + entNameToID.size() + " ents ");
		}
		
		@SuppressWarnings("unchecked")
		private synchronized HashSet<String> constructSet(String paths[], String cacheName, int colNum) throws IOException, ClassNotFoundException
		{
			final File cacheFile = new File(yagoCacheDir, cacheName);
			if (cacheFile.canRead()) {
				logger.info("loading cache from " + cacheFile);
				return (HashSet<String>) BinIO.loadObject(cacheFile);
			}
			HashSet<String> iValues = new HashSet<String>();
			for (String path : paths) {
				File csvFile = new File(yagoBaseDir, path);
				logger.info("reading " + csvFile);
				CSVReader csvr = new CSVReader(new FileReader(csvFile), '\t', (char) 0, 0);
				for (String line[] = null; (line = csvr.readNext()) != null; ) {
					iValues.add(line[colNum].trim());
				}
				csvr.close();
			}
			logger.info("loading "+iValues.size()+" wordnet cats");
			BinIO.storeObject(iValues, cacheFile);
			return iValues;
		}
		
		private synchronized StringIntBijection constructMap(String paths[], int colNum, HashSet<String> keys, String cacheName, HashSet<String> keySet, boolean include, String iPath) throws IOException, ClassNotFoundException {
			final File cacheFile = new File(yagoCacheDir, cacheName + "Map");
			if (cacheFile.canRead()) {
				logger.info("loading cache from " + cacheFile);
				return (StringIntBijection) BinIO.loadObject(cacheFile);
			}
			// !isCacheReadable
			keys.clear();
			for (String path : paths) {
				File csvFile = new File(yagoBaseDir, path);
				logger.info("reading " + csvFile);
				CSVReader csvr = new CSVReader(new FileReader(csvFile), '\t', (char) 0, 0);
				for (String line[] = null; (line = csvr.readNext()) != null; ) {
					final String akey = line[colNum].trim();
					if (path.equals(iPath)) {
						if (keySet.contains(akey) == include)
							keys.add(akey);
					}
					else
						keys.add(akey);
				}
				csvr.close();
			}
			StringIntBijection ans = new StringIntBijection(keys);
			BinIO.storeObject(ans, cacheFile);
			return ans;
		}
		
		public synchronized void getEntToCatMap() {
			if(entToCats == null)
				getIsAMaps();
		}
		
		public synchronized void getCatToEntMap()	{
			if(catToEnts == null)
				getIsAMaps();
		}
		
		public synchronized void getIsAMaps() {
			if (catToEnts != null && entToCats != null) {
				return;
			}
			try {
				catToEnts = new TIntArrayList[numCats()];
				entToCats = new TIntArrayList[numEnts()];
				constructIsARels(isaCache,catToSubEntCache,subClass2Type);
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
			catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * Note that canonical entity name uses underscore between tokens whereas lemma tokens are space separated. 
		 * @param cacheName
		 * @throws IOException
		 * @throws ClassNotFoundException
		 */
		@SuppressWarnings("unchecked")
		synchronized void constructLemmaMaps() {
			if (entIDToLemmas != null && catIDToLemmas != null) {
				return;
			}
			constructCatNameMap();
			constructEntNameMap();
			try {
				final File entMeansCacheFile = new File(yagoCacheDir, entMeansCache);
				final File catMeansCacheFile = new File(yagoCacheDir, catMeansCache);
				if (entMeansCacheFile.canRead() && catMeansCacheFile.canRead()) {
					logger.info("loading " + entMeansCache + " from " + entMeansCacheFile);
					entIDToLemmas = (IntToStringSet) BinIO.loadObject(entMeansCacheFile);
					logger.info(entMeansCacheFile + " loaded with " + entIDToLemmas.numKeys() + " keys");
					logger.info("loading " + catMeansCache + " from " + catMeansCacheFile);
					catIDToLemmas = (IntToStringSet) BinIO.loadObject(catMeansCacheFile);
					logger.info(catMeansCacheFile + " loaded with " + catIDToLemmas.numKeys() + " keys");
				}
				else { // the hard way
					long payloadBytes = 0;
					// first add canonical names as lemmas, slightly fudged to look like ordinary text
					ArrayList<String>[] entIDToLemmasFlabby = new ArrayList[entNameToID.size()];
					Arrays.fill(entIDToLemmasFlabby, null);
					for (int entID = 0; entID < entNameToID.size(); ++entID) {
						final String approxLemma = canonicalToFreeText(entNameToID.intToString(entID));
						entIDToLemmasFlabby[entID] = new ArrayList<String>();
						entIDToLemmasFlabby[entID].add(approxLemma);
						payloadBytes += approxLemma.length();
					}
					ArrayList<String>[] catIDToLemmasFlabby = new ArrayList[catNameToID.size()];
					Arrays.fill(catIDToLemmasFlabby, null);
					for (int catID = 0; catID < catNameToID.size(); ++catID) {
						final String approxLemma = canonicalToFreeText(catNameToID.intToString(catID));
						catIDToLemmasFlabby[catID] = new ArrayList<String>();
						catIDToLemmasFlabby[catID].add(approxLemma);
						payloadBytes += approxLemma.length();
					}
					for (String path : meansPaths) { // for each means file
						File csvFile = new File(yagoBaseDir, path);
						logger.info("reading " + csvFile);
						CSVReader csvr = new CSVReader(new FileReader(csvFile), '\t', (char) 0, 0);
						for (String line[] = null; (line = csvr.readNext()) != null; ) {
							final String lemma = canonicalToFreeText(line[1].trim()), entOrCat = line[2].trim();
							/*
							 * Note, lemma may be mangled in various ways, but not the canonical name.
							 * entOrCat may be found in the ent dict or cat dict. None is error, should not happen.
							 */
							int ecid;
							if ((ecid = entNameToEntID(entOrCat)) >= 0) {
								if (!entIDToLemmasFlabby[ecid].contains(lemma)) {
									entIDToLemmasFlabby[ecid].add(lemma);
									payloadBytes += lemma.length();
								}
							}
							else if ((ecid = catNameToCatID(entOrCat)) >= 0) {
								if (!catIDToLemmasFlabby[ecid].contains(lemma)) {
									catIDToLemmasFlabby[ecid].add(lemma);
									payloadBytes += lemma.length();
								}
							}
							else {
								logger.warn("{" + entOrCat + "} found in neither ent nor cat dict");
							}
						}
						csvr.close();
					} // for each means file
					logger.info("read " + payloadBytes + " bytes of means strings");
					entIDToLemmas = new IntToStringSet(entIDToLemmasFlabby);
					BinIO.storeObject(entIDToLemmas, new File(yagoCacheDir, entMeansCache));
					catIDToLemmas = new IntToStringSet(catIDToLemmasFlabby);
					BinIO.storeObject(catIDToLemmas, new File(yagoCacheDir, catMeansCache));
				}
			}
			catch (IOException iox) {
				iox.printStackTrace();
			} catch (ClassNotFoundException cnfx) {
				cnfx.printStackTrace();
			}
		}
		
		/**
		 * Constructed from entIDtoLemmas.
		 */
		synchronized void constructLemmaToEntMap() {
			if (lemmaToEntIDs != null) return;
			constructLemmaMaps();
			logger.info("constructLemmaToEntMap");
			lemmaToEntIDs = new HashMap<String, TIntArrayList>(entIDToLemmas.numKeys());
			MutableString lemmaString = new MutableString(128); 
			for (int entID = 0; entID < entIDToLemmas.numKeys(); ++entID) {
				final int numLemmas = entIDToLemmas.keyToNumValues(entID);
				for (int lx = 0; lx < numLemmas; ++lx) {
					entIDToLemmas.keyToValue(entID, lx, lemmaString);
					final String lemma = lemmaString.toString();
					if (!lemmaToEntIDs.containsKey(lemma)) {
						lemmaToEntIDs.put(lemma, new TIntArrayList());
					}
					if (!lemmaToEntIDs.get(lemma).contains(entID)) {
						lemmaToEntIDs.get(lemma).add(entID);
					}
				}
			}
		}

		@SuppressWarnings("unchecked")
		synchronized void constructIsARels(String ent2CatCacheName, String cat2EntCacheName, String s2tSet) throws IOException, ClassNotFoundException {
			final File ent2CatCacheFile = new File(yagoCacheDir, ent2CatCacheName);
			final File cat2EntCacheFile = new File(yagoCacheDir, cat2EntCacheName);
			
			if (ent2CatCacheFile.canRead() && cat2EntCacheFile.canRead()) {
				TIntArrayList[] _catToEnts = (TIntArrayList[]) BinIO.loadObject(cat2EntCacheFile);
				System.arraycopy(_catToEnts, 0, catToEnts, 0, _catToEnts.length);
				logger.info("loading cache from " + cat2EntCacheFile);
				TIntArrayList[] _entToCats = (TIntArrayList[]) BinIO.loadObject(ent2CatCacheFile);
				System.arraycopy(_entToCats, 0, entToCats, 0, _entToCats.length);
				logger.info("loading cache from " + ent2CatCacheFile);
				return;
			}
			long isaRel = 0, notFound = 0;
			entToCats = new TIntArrayList[entNameToID.size()];
			catToEnts = new TIntArrayList[catNameToID.size()];
			for (String path : isaPaths) {
				File csvFile = new File(yagoBaseDir, path);
				logger.info("reading " + csvFile);
				CSVReader csvr = new CSVReader(new FileReader(csvFile), '\t', (char) 0, 0);
				for (String line[] = null; (line = csvr.readNext()) != null; ) {
					final String ent = line[1], cat = line[2];
					if (!entNameToID.containsKey(ent) || !catNameToID.containsKey(cat)) {
						++notFound;
						continue;
					}
					++isaRel;
					final int entID = (int) entNameToID.getLong(ent), catID = (int) catNameToID.getLong(cat);
					if (entToCats[entID] == null) {
						entToCats[entID] = new TIntArrayList(); 
					}
					entToCats[entID].add(catID);
					if (catToEnts[catID] == null) {
						catToEnts[catID] = new TIntArrayList();
					}
					catToEnts[catID].add(entID);
				}
				csvr.close();
			}
			dedup(entToCats, "entToCats");
			dedup(catToEnts, "catToEnts");
			
			final File s2tSetFile = new File(yagoCacheDir, s2tSet);
			int cnf = 0, cf=0;
			if(s2tSetFile.canRead()) {
				HashMap<String, String> s2tMap = (HashMap<String, String>) BinIO.loadObject(s2tSetFile);
				Set<String> keySet = s2tMap.keySet();
				Iterator<String> itr = keySet.iterator();
				while(itr.hasNext())
				{
					String cat = itr.next();
					String ent = s2tMap.get(cat);
					if(!catNameToID.containsKey(cat) || !entNameToID.containsKey(ent))
					{
						++cnf;
						continue;
					}
					++cf;
					checkInsert(catToEnts, (int)catNameToID.getLong(cat), (int)entNameToID.getLong(ent));
				}
				
				logger.info(cf+"loaded "+cnf+" not found");
			}
			else {
				logger.info("s2tSet not loaded");
			}
			logger.info("loaded " + isaRel + " isa maps, " + notFound + " not found");
			BinIO.storeObject(catToEnts, cat2EntCacheFile);
			BinIO.storeObject(entToCats, ent2CatCacheFile);
		}
		
		synchronized void constructSubSupCats(/*String subCacheName, String supCacheName, String s2tSet*/) {
			try {
				if (catToSubCats != null && catToSupCats != null) {
					return;
				}
				final String subCacheName = catSubCache;
				final String supCacheName = catSupCache;
				final File subCacheFile = new File(yagoCacheDir, subCacheName);
				final File supCacheFile = new File(yagoCacheDir, supCacheName);

				catToSubCats = new TIntArrayList[catNameToID.size()];
				catToSupCats = new TIntArrayList[catNameToID.size()];

				if (subCacheFile.canRead() && supCacheFile.canRead()) {
					TIntArrayList[] _catToSubCats = (TIntArrayList[]) BinIO.loadObject(subCacheFile);
					System.arraycopy(_catToSubCats, 0, catToSubCats, 0, _catToSubCats.length);
					logger.info("loading cache from " + subCacheFile);
					TIntArrayList[] _catToSupCats = (TIntArrayList[]) BinIO.loadObject(supCacheFile);
					System.arraycopy(_catToSupCats, 0, catToSupCats, 0, _catToSupCats.length);
					logger.info("loading cache from " + supCacheFile);
					return;
				}

				HashMap<String, String> subClassToType = new HashMap<String, String>();
				long subSupCat = 0, notFound = 0, s2t = 0;
				for (String path : subCatPaths) {
					File csvFile = new File(yagoBaseDir, path);
					logger.info("reading " + csvFile);
					CSVReader csvr = new CSVReader(new FileReader(csvFile), '\t', (char) 0, 0);
					for (String line[] = null; (line = csvr.readNext()) != null; ) {
						final String subCat = line[1], cat = line[2];
						if(catNameToID.containsKey(cat) && entNameToID.containsKey(subCat))
						{
							subClassToType.put(cat, subCat);
							++s2t;
							continue;
						}
						if (!catNameToID.containsKey(cat) || !catNameToID.containsKey(subCat)) {
							++notFound;
							continue;
						}
						++subSupCat;
						final int subCatID = (int) catNameToID.getLong(subCat), catID = (int) catNameToID.getLong(cat);
						if (catToSubCats[catID] == null) {
							catToSubCats[catID] = new TIntArrayList();
						}
						catToSubCats[catID].add(subCatID);
						if (catToSupCats[subCatID] == null) {
							catToSupCats[subCatID] = new TIntArrayList();
						}
						catToSupCats[subCatID].add(catID);
					}
					csvr.close();
				}
				dedup(catToSubCats, "catToSubCats");
				dedup(catToSupCats, "catToSupCats");

				logger.info("loaded " + subSupCat + " sub/sup cat maps, " + s2t+" converted "+notFound + " not found");
				BinIO.storeObject(catToSubCats, new File(yagoCacheDir, subCacheName));
				BinIO.storeObject(catToSupCats, new File(yagoCacheDir, supCacheName));
				BinIO.storeObject(subClassToType, new File(yagoCacheDir, subClass2Type));
			}
			catch (IOException iox) {
				logger.fatal(iox.getMessage());
			}
			catch (ClassNotFoundException cnfx) {
				logger.fatal(cnfx.getMessage());
			}
		}
		
		synchronized void dedup(TIntArrayList[] dup, String name) {
			logger.info("deduplicating " + name);
			for (int dx = 0; dx < dup.length; ++dx) {
				dup[dx] = dedup(dup[dx]);
			}
		}
		
		synchronized TIntArrayList dedup(TIntArrayList dup) {
			if (dup == null) {
				return dup;
			}
			dup.sort();
			if (dup.size() > 1) {
				for (int rx = 1; rx < dup.size(); ++rx) {
					if (dup.get(rx) == dup.get(rx-1)) {
						dup.remove(rx);
					}
				}
			}
			return dup;
		}
		
		synchronized void checkInsert(TIntArrayList[] lol, int index, int val) {
			if (lol[index] == null) {
				lol[index] = new TIntArrayList();
			}
			if (!lol[index].contains(val)) {
				lol[index].add(val);
			}
		}
	} // factory
}
