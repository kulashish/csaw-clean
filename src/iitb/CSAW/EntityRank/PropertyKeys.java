package iitb.CSAW.EntityRank;

import org.apache.commons.configuration.PropertiesConfiguration;

/**
 * Stores final {@link String} constants with which to look up 
 * {@link PropertiesConfiguration}.
 * @author soumen
 */
public class PropertyKeys {
	public static final String batchQueryDirKey = "batchQueryDir";
	public static final String windowKey = "intervalWindow";
	
	public static final String snippetDirKey = "SnippetDir";
	public static final String snippetFeatureVectorPrefix = "SnippetFeatureVector";
	public static final String snippetFeatureVectorSuffix = ".dat";
	
	public static final String rawSnippetFileKey = "rawSnippetFile";
	public static final String sortedSnippetFileKey = "sortedSnippetFile";
	
	public static final String snippetHighLevelFeaturesKey = "snippetHighLevelFeatures";
	public static final String snippetFeaturesKey = "snippetFeatures";
	
	public static final String svmCeeKey = "svmCee";
	
	public static final String idfSlots = "idfSlots";
	public static final String proximitySlots = "proximitySlots";
}
