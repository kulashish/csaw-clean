package iitb.CSAW.Catalog.DBpedia;

import gnu.trove.TIntArrayList;
import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.MemoryStatus;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.openjena.riot.RiotReader;
import org.openjena.riot.lang.LangNTriples;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

/**
 * Catalog provider for Dbpedia. Reads the raw dbpedia data files, creates and stores the required maps. 
 * Provides access methods for finding the entity to id, category to id and entity to category mappings. 
 * <p>This class is thread-safe.
 * <p><b>Note:</b> There is no mapping to lemmas. Calling lemma map related methods will throw the {@link NotImplementedException}
 * @author devshree
 * @since 11 January 2011
 * 
 */
public class DbpediaCatalog extends ACatalog {
    public static void main(String[] args) throws Exception {
    	Config config = new Config(args[0], args[1]);
    	ACatalog catalog = ACatalog.construct(config);
    	MemoryStatus ms = new MemoryStatus();
    	System.out.println(ms);
    	System.out.println(catalog.numCats());
    	System.out.println(catalog.numEnts());
    	TIntHashSet set = new TIntHashSet();
    	catalog.entsReachableFromCat(0, set);
    	set.clear();
    	catalog.catsReachableFromEnt(0, set);
    	System.gc();
    	System.out.println(ms);
    }

    private final static Logger logger = Logger.getLogger(DbpediaCatalog.class);
    private final Config config;
    private static File DbpediaBaseDir;
    private static File DbpediaCacheDir;
    private static DbpediaCatalog dbpc = null;
    private static final String DbpediaCacheName = "cache";
    
    
    public static synchronized ACatalog getInstance(Config config) throws IOException {
    	if (dbpc == null) {
    		dbpc = new DbpediaCatalog(config);
    	}
    	return dbpc;
    }

    DbpediaCatalog(Config config) throws IOException {
    	this.config = config;
    	this.config.setThrowExceptionOnMissing(true);
    	DbpediaBaseDir = new File(this.config.getString(iitb.CSAW.Catalog.PropertyKeys.dbpediaBaseDir));
    	DbpediaCacheDir = new File(DbpediaBaseDir, DbpediaCacheName);
    	createMapHolder();
    	
    }
    
    /* public access points */
    
    /**
     * Returns the category name associated with catID. 
     */
    @Override
    public String catIDToCatName(int catId)
    {
	loadMap(catNameToId, catCache);
	if(catId >= numCats())
	    throw new IllegalArgumentException("category id "+catId+" is greater than category name map size "+numCats());
	return catNameToId.intToString(catId);
    }

     /**
     * Returns the id in the map corresponding to the category name. Returns -1 if the catName is not in map
     */
    @Override
    public int catNameToCatID(String catName)
    {
	loadMap(catNameToId, catCache);
	return catNameToId.getInt(catName);
    }

    /**
     * Finds all categories that are transitively reachable from a entity
     */
    @Override
    public void catsReachableFromEnt(int entId, final TIntHashSet catIds)
    {
	loadMap(entToCats, entToSupCatCache);
	entToCats[entId].forEach(new TIntProcedure()
	{
	    @Override
	    public boolean execute(int value)
	    {
		collectSupCatIds(value, catIds);
		return true;
	    }
	});
    }

    /**
     * Returns the entity name associated with entID. 
     */
    @Override
    public String entIDToEntName(int entId)
    {
	loadMap(entNameToId, entCache);
	if(entId > numEnts())
	    throw new IllegalArgumentException("entity id "+entId+" is greater than entity name map size "+numEnts());
	return entNameToId.intToString(entId);
    }

    /**
     * Returns the id in the map corresponding to the entity name. Returns -1 if entName is not in map
     */
    @Override
    public int entNameToEntID(String entName)
    {
	loadMap(entNameToId, entCache);
	return entNameToId.getInt(entName);
	
    }

    /**
     * Finds all entities that are transitively reachable from a category
     */
    @Override
    public void entsReachableFromCat(int catID, final TIntHashSet entIDs)
    {
	loadMap(catToEnts, catToSubEntCache);
	loadMap(catToSubCats, catSubCache);
	TIntHashSet catIds = new TIntHashSet();
	collectSubCatIds(catID, catIds);
	catIds.forEach(new TIntProcedure()
	{
	    @Override
	    public boolean execute(int value)
	    {
		entIDs.addAll(catToEnts[value].toNativeArray());
		return true;
	    }
	});
    }

    /**
     * Returns the number of categories in DBpedia
     */
    @Override
    public int numCats()
    {
	loadMap(catNameToId, catCache);
	return catNameToId.size();
    }

    /**
     * Returns the number of entities i.e. concepts in DBpedia
     */
    @Override
    public int numEnts()
    {
	loadMap(entNameToId, entCache);
	return entNameToId.size();
    }

    public void supCatsReachableFromCat(int catId, TIntHashSet catIds)
    {
	loadMap(catToSupCats, catSupCache);
	if(catId >= numCats())
	    throw new IllegalArgumentException("category id "+catId+" is greater than category name map size "+numCats());
	collectSupCatIds(catId, catIds);
    }
    
    @Override
    public void subCatsReachableFromCat(int catId, TIntHashSet catIds)
    {
	loadMap(catToSubCats, catSubCache);
	if(catId >= numCats())
	    throw new IllegalArgumentException("category id "+catId+" is greater than category name map size "+numCats());
	collectSubCatIds(catId, catIds);
    }

    /**
     * pre-condition: {@link DbpediaCatalog#catToSupCats} is not null
     */
    private void collectSupCatIds(final int catId, final TIntHashSet catIds) 
    {
    	loadMap(catToSupCats, catSupCache);
	assert catToSupCats!=null;
	catIds.add(catId);
	if (catToSupCats[catId] == null) {
		return;
	}
	catToSupCats[catId].forEach(new TIntProcedure() {
		@Override
		public boolean execute(int supCatId) 
		{
			logger.debug("\t" + catIDToCatName(catId) + " subCatOf " + catIDToCatName(supCatId));
			collectSupCatIds(supCatId, catIds);
			return true;
		}
	});
    }
    
    /**
     * pre-condition: {@link DbpediaCatalog#catToSubCats} is not null
     */
    private void collectSubCatIds(final int catId, final TIntHashSet catIds) 
    {
    	loadMap(catToSubCats, catSubCache);
	assert catToSubCats!=null;
	catIds.add(catId);
	if (catToSubCats[catId] == null) {
		return;
	}
	catToSubCats[catId].forEach(new TIntProcedure() {
		@Override
		public boolean execute(int subCatId) 
		{
			logger.debug("\t" + catIDToCatName(catId) + " supCatOf " + catIDToCatName(subCatId));
			collectSubCatIds(subCatId, catIds);
			return true;
		}
	});
    }
    
    /**
     * If map is null, then calls the appropriate map creation/loading method
     * @param map
     * @param mapCacheName
     */
    private synchronized void loadMap(Object map, String mapCacheName)
    {
	if (map == null)
	    try
	    {
		Callable<Object> mapObj = maps.get(mapCacheName);
		mapObj.call();
	    } 
	    catch(Exception e)
	    {
		throw new RuntimeException("Could not create map "+mapCacheName);
	    }
	    
    }
    
    /* Data holders */
    
    private static StringIntBijection catNameToId;
    private static TIntArrayList[] catToSubCats;
    private static TIntArrayList[] catToSupCats;
    
    private static StringIntBijection entNameToId;
    private static TIntArrayList[] entToCats;
    private static TIntArrayList[] catToEnts;
    
    /* Constants */
    
    private static final String catToSupCatRelationName = "subClassOf";
    private static final String ontologyFileName = "dbpedia_3.5.1.owl";
    private static final String artToCatFileName = "instance_types_en.nt";
    private static final String catCache = "catCache";
    private static final String catSubCache = "catSubCache";
    private static final String catSupCache = "catSupCache";
    private static final String entCache = "entCache";
    private static final String entToSupCatCache = "entToSupCatCache";
    private static final String catToSubEntCache = "catToSubEntCache";
    
    private static Map<String, Callable<Object>> maps;
    
    /* Private thread-safe map creation methods */
    
    private static synchronized void createMapHolder()
    {
	maps = new HashMap<String, Callable<Object>>();
	maps.put(catCache, new Callable<Object>()
		{
	   	 public Object call() throws IOException, ClassNotFoundException
	   	 {
	   	     createCatNameMap();
	   	     return null;
	   	 }
		});
	
	maps.put(entCache, new Callable<Object>()
		{
	    	public Object call() throws IOException, ClassNotFoundException
	    	{
	    	    createEntNameMap();
	    	    return null;
	    	}
		});
	
	maps.put(catSubCache, new Callable<Object>()
		{
		    public Object call() throws IOException, ClassNotFoundException
		    {
			createCat2CatMaps();
			return null;
		    }
		});
	maps.put(catSupCache, new Callable<Object>()
		{
		    public Object call() throws IOException, ClassNotFoundException
		    {
			createCat2CatMaps();
			return null;
		    }
		});
	maps.put(catToSubEntCache, new Callable<Object>()
		{
		    public Object call() throws IOException, ClassNotFoundException
		    {
			createEnt2CatMaps();
			return null;
		    }
		});
	maps.put(entToSupCatCache, new Callable<Object>()
		{
		    public Object call() throws IOException, ClassNotFoundException
		    {
			createEnt2CatMaps();
			return null;
		    }
		});
	
    }
    
    /**
     * Parses the dbpedia ontology file , creates and stores the category name to id map
     */
    private static synchronized void createCatNameMap() throws IOException, ClassNotFoundException
    {
	if(catNameToId != null) return;
	File mapFile = new File(DbpediaCacheDir, catCache);
	if(mapFile.canRead())
	{
	   logger.info("Loading category name to id map from "+mapFile);
	   catNameToId = (StringIntBijection) BinIO.loadObject(mapFile); 
	   return;
	}
	File ontologyFile = new File(DbpediaBaseDir, ontologyFileName);
	StmtIterator si = getStmtIterator(ontologyFile);
	ObjectOpenHashSet<String> catNames = new ObjectOpenHashSet<String>();
	MutableString catName = new MutableString();
	MutableString supCatName = new MutableString();
	logger.info("Creating category name map");
	while(si.hasNext())
	{
		final Statement s = si.next();
		final String predicate = s.getPredicate().getLocalName();
		if(predicate.equals(catToSupCatRelationName))
		{
		    getNodeNames(s, catName, supCatName);
		    if(!catName.equals("")) catNames.add(catName.toString());
		    if(!supCatName.equals("")) catNames.add(supCatName.toString());
		}
	}
	si.close();
	logger.info("Collected "+catNames.size()+" categories, creating category name to id map");
	catNameToId = new StringIntBijection(catNames);
	if(!DbpediaCacheDir.exists())
	    DbpediaCacheDir.mkdir();
	logger.info("Saving category name to id map to "+mapFile);
	BinIO.storeObject(catNameToId, mapFile);
    }
    
    /**
     * Parses the dbpdia ontology file, creates and stores
     * <li>Category to super Category Map {@link DbpediaCatalog#catToSupCats}
     * <li>Category to subsuming Category Map {@link DbpediaCatalog#catToSubCats}
     */
    private static synchronized void createCat2CatMaps() throws IOException, ClassNotFoundException
    {
	if(!(catToSubCats == null || catToSupCats == null)) return;
	File subCatCacheFile = new File(DbpediaCacheDir, catSubCache);
	File supCatCacheFile = new File(DbpediaCacheDir, catSupCache);
	if(subCatCacheFile.canRead() && supCatCacheFile.canRead())
	{
	    catToSubCats = (TIntArrayList[]) BinIO.loadObject(subCatCacheFile);
	    catToSupCats = (TIntArrayList[]) BinIO.loadObject(supCatCacheFile);
	    return;
	}
	
	createCatNameMap();
	File ontologyFile = new File(DbpediaBaseDir, ontologyFileName);
	StmtIterator si = getStmtIterator(ontologyFile);
	
	catToSubCats = new TIntArrayList[catNameToId.size()];
	catToSupCats = new TIntArrayList[catNameToId.size()];
	for(int i=0;i<catNameToId.size();i++)
	{
	    catToSubCats[i] = new TIntArrayList();
	    catToSupCats[i] = new TIntArrayList();
	}
	int sube = 0, supe = 0;
	MutableString catName = new MutableString();
	MutableString supCatName = new MutableString();
	while(si.hasNext())
	{
		final Statement s = si.next();
		final String predicate = s.getPredicate().getLocalName();
		if(predicate.equals(catToSupCatRelationName))
		{
		    getNodeNames(s, catName, supCatName);
		    if(checkAndInsert(catName.toString(), supCatName.toString(), catNameToId, catNameToId, catToSupCats))
			supe++;
		    if(checkAndInsert(supCatName.toString(), catName.toString(), catNameToId, catNameToId, catToSubCats))
			sube++;
		}
	}
	si.close();
	logger.info("Category to category maps created, sub edges = "+sube+" sup edges = "+supe);
	logger.info("Storing sub category map at "+subCatCacheFile);
	BinIO.storeObject(catToSubCats, subCatCacheFile);
	logger.info("Storing sup category map at "+supCatCacheFile);
	BinIO.storeObject(catToSupCats, supCatCacheFile);
    }
    
    /**
     * Parses the article to category map rdf file from dbpedia, creates and stores the entity name to id map 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static synchronized void createEntNameMap() throws IOException, ClassNotFoundException
    {
	if(entNameToId != null) return;
	File entityCacheFile = new File(DbpediaCacheDir, entCache);
	if(entityCacheFile.canRead())
	{
	    logger.info("Loading ent name map from "+entityCacheFile);
	    entNameToId = (StringIntBijection) BinIO.loadObject(entityCacheFile);
	    return;
	}
	logger.info("Creating entity name map");
	File artToCatFile = new File(DbpediaBaseDir, artToCatFileName);
	LangNTriples lntriples = RiotReader.createParserNTriples(new FileInputStream(artToCatFile), null);
	ObjectOpenHashSet<String> entNames = new ObjectOpenHashSet<String>();
	while(lntriples.hasNext())
	{
	    final Triple t = lntriples.next();
	    
	    /*
	     * Workaround for bug in Jena parsing. Node.getLocalName(..) returns blank string when 
	     * the URI contains a % character. (Many URIs contain hex characters)
	     */
	    
	    final String subNameStr = t.getSubject().getURI();
	    int lastIndex = subNameStr.lastIndexOf("/");
	    final String subName = subNameStr.substring(lastIndex+1, subNameStr.length());
	    final String entName = URLDecoder.decode(subName, "UTF-8");
	    if(!entName.equals(""))
		entNames.add(entName);
	    else
		logger.warn("Blank entity name for "+t.toString()+" for subject name "+subName);
	}
	logger.info("Collected "+entNames.size()+" entities");
	entNameToId = new StringIntBijection(entNames); 
	logger.info("Storing entity name map at "+entityCacheFile);
	if(!DbpediaCacheDir.exists())
	    DbpediaCacheDir.mkdir();
	BinIO.storeObject(entNameToId, entityCacheFile);
    }
    
    /**
     * Parses the article to category map rdf file from dbpedia, creates and stores 
     * <li>Entity to super category map {@link DbpediaCatalog#entToCats}
     * <li>Category to subsuming entity map {@link DbpediaCatalog#catToEnts}
     */
    private static synchronized void createEnt2CatMaps() throws IOException, ClassNotFoundException
    {
	if(!(entToCats == null || catToEnts == null)) return;
	File entToSupCatCacheFile = new File(DbpediaCacheDir, entToSupCatCache);
	File catToSubEntCacheFile = new File(DbpediaCacheDir, catToSubEntCache);
	if(entToSupCatCacheFile.canRead() && catToSubEntCacheFile.canRead())
	{
	    logger.info("Loading entity to category map from "+entToSupCatCacheFile);
	    entToCats = (TIntArrayList[]) BinIO.loadObject(entToSupCatCacheFile);
	    logger.info("Loading category to sub entity map from "+catToSubEntCacheFile);
	    catToEnts = (TIntArrayList[]) BinIO.loadObject(catToSubEntCacheFile);
	    return;
	}
	createEntNameMap();
	createCatNameMap();
	File artToCatFile = new File(DbpediaBaseDir, artToCatFileName);
	entToCats = new TIntArrayList[entNameToId.size()];
	catToEnts = new TIntArrayList[catNameToId.size()];
	for(int i=0;i<entNameToId.size();i++)
	    entToCats[i] = new TIntArrayList();
	for(int i=0;i<catNameToId.size();i++)
	    catToEnts[i] = new TIntArrayList();
	LangNTriples lntriples = RiotReader.createParserNTriples(new FileInputStream(artToCatFile), null);
	logger.info("Creating entity to category maps");
	int e2c=0, c2e=0;
	while(lntriples.hasNext())
	{
	    final Triple t = lntriples.next();
	    /*
	     * Workaround for bug in Jena parsing. Node.getLocalName(..) returns blank string when 
	     * the URI contains a % character. (Many URIs contain hex characters)
	     */
	    final String subNameStr = t.getSubject().getURI();
	    int lastIndex = subNameStr.lastIndexOf("/");
	    final String subName = subNameStr.substring(lastIndex+1, subNameStr.length());
	    final String entName = URLDecoder.decode(subName, "UTF-8");
	    final String catName = URLDecoder.decode(t.getObject().getLocalName(), "UTF-8");
	    if(checkAndInsert(entName, catName, entNameToId, catNameToId, entToCats))
		    e2c++;
	    if(checkAndInsert(catName, entName, catNameToId, entNameToId, catToEnts))
		    c2e++;
	}
	logger.info("Added "+e2c+" entity to category edges, "+c2e+" category to entity edges");
	logger.info("Storing entity to super category map at "+entToSupCatCacheFile);
	BinIO.storeObject(entToCats, entToSupCatCacheFile);
	logger.info("Storing category to sub entity map at "+catToSubEntCacheFile);
	BinIO.storeObject(catToEnts, catToSubEntCacheFile);
    }
    
       
 /**
     * Adds a relation to the relationMap, only when both arg1 and arg2 are present in the respective nameMaps. 
     * @param arg1
     * @param arg2
     * @param nameMap
     * @param relationMap
     */
    private static synchronized boolean checkAndInsert(String arg1, String arg2, StringIntBijection nameMap1, StringIntBijection nameMap2, TIntArrayList[] relationMap)
    {
	final int id1 = nameMap1.getInt(arg1);
	if(id1 >=0 )
	{
	    final int id2 = nameMap2.getInt(arg2);
	    if(id2 >=0)
	    {
		relationMap[id1].add(id2);
		return true;
	    }
	    else
	    {
		    logger.warn("Argument "+arg2 +" not in namemap");
		    return false;
	    }
	}
	else
	{
	    logger.warn("Argument "+arg1 +" not in namemap");
	    return false;
	}
    }
    /**
     * Returns an iterator over statements of the ontology read from the ontologyFile
     * @param ontologyFile
     */
    private static StmtIterator getStmtIterator(File ontologyFile) throws FileNotFoundException
    {
	logger.info("Reading "+ ontologyFile);
	Model model = ModelFactory.createDefaultModel();
	model.read(new FileInputStream(ontologyFile), null);
	return model.listStatements();
    }
    
    /**
     * Finds the node local names of the subject and object of the input {@link Statement} 
     * @param s statement, represents an edge in an ontology
     */
    private static void getNodeNames(Statement s, MutableString subjectName, MutableString objectName)
    {
	subjectName.length(0);
	objectName.length(0);
	try
	{
		subjectName.append(URLDecoder.decode(s.getSubject().getLocalName(), "UTF-8"));
	}
	catch (UnsupportedEncodingException e)
	{
	    logger.warn(e + " occurred for "+s.getSubject().getLocalName());
	}
	try
	{
	    objectName.append(URLDecoder.decode(s.getObject().asNode().getLocalName(), "UTF-8"));
	} 
	catch (UnsupportedEncodingException e)
	{
	    logger.warn(e + " occurred for "+s.getObject().asNode().getLocalName());
	}
    }
    
    /* Unimplemented methods */
    @Override
    public void catIDToLemma(int catID, int lemmaOfs, MutableString ans)
    {
	throw new NotImplementedException();
	
    }

    @Override
    public int catIDToNumLemmas(int catID)
    {
	throw new NotImplementedException();
    }

    @Override
    public void entIDToLemma(int entID, int lemmaOfs, MutableString ans)
    {
	throw new NotImplementedException();
    }

    @Override
    public int entIDToNumLemmas(int entID) {
    	return 0;
    }

	@Override
	public TIntHashSet rootCats() {
		throw new NotImplementedException();
	}

	@Override
	public String canonicalToFreeText(String canon) {
		throw new NotImplementedException();
	}
}
