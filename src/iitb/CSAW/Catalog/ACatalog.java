package iitb.CSAW.Catalog;

import java.lang.reflect.InvocationTargetException;

import gnu.trove.TIntHashSet;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.lang.MutableString;

public abstract class ACatalog {
	public static ACatalog construct(Config config) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		return (ACatalog) Class.forName(config.getString(iitb.CSAW.Catalog.PropertyKeys.catalogProviderKey)).getMethod("getInstance", Config.class).invoke(null, config);
	}
	
	public abstract TIntHashSet rootCats();
	public abstract int numCats();
	public abstract int catNameToCatID(String catName);
	/**
	 * @param catID
	 * @return canonical category name
	 */
	public abstract String catIDToCatName(int catID);
	public abstract int catIDToNumLemmas(int catID);
	/**
	 * @param catID
	 * @param lemmaOfs
	 * @param ans output, not canonical but could be "ordinary free text"
	 */
	public abstract void catIDToLemma(int catID, int lemmaOfs, MutableString ans);
	
	public abstract int numEnts();
	public abstract int entNameToEntID(String entName);
	/**
	 * @param entID
	 * @return canonical entity name
	 */
	public abstract String entIDToEntName(int entID);
	public abstract int entIDToNumLemmas(int entID);
	/**
	 * @param entID
	 * @param lemmaOfs
	 * @param ans output, not canonical but could be "ordinary free text"
	 */
	public abstract void entIDToLemma(int entID, int lemmaOfs, MutableString ans);
	
	/**
	 * @param canon canonical ent or cat name as defined in catalog,
	 * e.g., wikicategory_Catalan_football or wordnet_rush_111743294
	 * or wikicategory_Football_(soccer)_goalkeepers
	 * @return free text form, e.g., Catalan_football or rush or
	 * Football_(soccer)_goalkeepers. The exact policy is up to the 
	 * implementer of {@link ACatalog}. E.g. one may choose to remove
	 * (soccer) above.
	 */
	public abstract String canonicalToFreeText(String canon);
		
	public abstract void entsReachableFromCat(int catID, TIntHashSet entIDs);
	public abstract void subCatsReachableFromCat(int catId, TIntHashSet catIds);
	public abstract void catsReachableFromEnt(int entId, final TIntHashSet catIds);
}
