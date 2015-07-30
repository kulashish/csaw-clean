package iitb.CSAW.Spotter;

public class PropertyKeys {
	public static final String tokenCountsDirName = "Spotter.TokenCountsDir";
	public static final String contextWindowName = "Spotter.ContextWindow";
	public static final String contextRetainNormName = "Spotter.ContextRetainNorm";
	
	public static final String contextBaseDirKey1 = "Spotter.ContextBaseDir1";
	public static final String contextBaseDirKey2 = "Spotter.ContextBaseDir2";

//	public static final String contextBaseDirName1 = "Spotter.ContextBaseDir1";
//	public static final String contextBaseDirName2 = "Spotter.ContextBaseDir2";
//	public static final String contextBaseDirName3 = "Spotter.ContextBaseDir3";
	
	private static final String lfem = LeafFeatureEntityMaps.class.getCanonicalName();
	public static final String r0Key = lfem+".r0", r1Key = lfem+".r1", s0Key = lfem+".s0", s1Key = lfem+".s1";

	/*
	 * Mentions
	 */
	public static final String mentionsFileName = "Spotter.MentionsFile";
	public static final String mergedMentionsFileName = "Spotter.MergedMentionsFile";
	public static final String phraseCountsFileName = "Spotter.PhraseCountsFile";
	public static final String cleanedMentionsFileName = "Spotter.CleanedMentionsFile";
	public static final String discardedMentionsFileKey = "Spotter.DiscardedMentionsFile";
	public static final String minAnnotProbName = "Spotter.MinAnnotProb";
	
	/*
	 * 2010/12/01 soumen
	 * 2012/06/16 soumen
	 * 
	 * The following keys were used by DS to build a tripartite graph of words,
	 * lemmas, and entities for UTA.  These have been removed.
	 * tokensFile = "Wikipedia.Tokens";
	 * lemmasFile = "Wikipedia.Lemmas";
	 * entitiesFile = "Wikipedia.Entities";
	 * tokenToLemmasFile = "Wikipedia.TokensToLemmas";
	 * groundTruthDir = "Spotter.Groundtruth.dir";
	 * groundTruthCorpus = "Spotter.Groundtruth.rar";
	 * graphDataBaseDirName = "Wikipedia.GraphDataBaseDir";
	 * lemmasToEntitiesFile = "Wikipedia.LemmasToEntities";
	 * spotterBaseDir = "Spotter.Basedir";
	 * entlemmasetsFile = "Spotter.EntLemmasetsFile";
	 * lemmasFile = "Spotter.YagoLemmas";
	 * lemmaTermMapFile = "Spotter.LemmaTermMap";
	 * corpusTermMapFile = "Spotter.CorpusTermMap";
	 * 
	 * The definition of DELIMITERS has also been removed.
	 */
}
