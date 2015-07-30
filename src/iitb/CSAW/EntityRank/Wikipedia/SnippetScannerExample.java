package iitb.CSAW.EntityRank.Wikipedia;

import iitb.CSAW.EntityRank.Feature.AFeature;
import iitb.CSAW.EntityRank.InexTrec.QueryWithAnswers;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.mutable.MutableInt;

import com.jamonapi.MonitorFactory;

public class SnippetScannerExample extends SnippetScannerBase implements ISnippetHandler {
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		SnippetScannerExample sse = new SnippetScannerExample(conf);
		sse.run();
	}
	
	final HashMap<String, QueryWithAnswers> queries = new HashMap<String, QueryWithAnswers>();
	static final boolean doFeatures = false;

	SnippetScannerExample(Config conf) throws Exception {
		super(conf);
		QueryWithAnswers.load(conf, queries);
		for (QueryWithAnswers query : queries.values()) {
			decorate(query.rootQuery);
		}
	}
	
	void run() throws Exception {
		scanAll(this);
		logger.info(MonitorFactory.getMonitor("goodScore", ""));
		logger.info(MonitorFactory.getMonitor("badScore", ""));
		logger.info(MonitorFactory.getMonitor("numSnip", ""));
		logger.info(MonitorFactory.getMonitor("hasGoodBad", ""));
		logger.info(MonitorFactory.getMonitor("numCandidate", ""));
		logger.info(MonitorFactory.getMonitor("firstGoodRank", ""));
		logger.info(MonitorFactory.getMonitor("numDistinctScores", ""));
	}
	
	@Override
	public void handleOneQuery(ArrayList<Snippet> snippets) {
		HashSet<String> posEnts = new HashSet<String>(), negEnts = new HashSet<String>();
		MutableInt nGoodSnippets = new MutableInt(), nBadSnippets = new MutableInt();
		collectStatistics(snippets, nGoodSnippets, nBadSnippets, posEnts, negEnts);
		final boolean hasGoodBad = nGoodSnippets.intValue() > 0 && nBadSnippets.intValue() > 0;
		MonitorFactory.add("hasGoodBad", "", hasGoodBad? 1 : 0);
		MonitorFactory.add("numCandidate", "", posEnts.size() + negEnts.size()); // assuming an ent can't be both
		if (!hasGoodBad) {
			return;
		}
		final String queryId = snippets.get(0).queryId;
		if (!queries.containsKey(queryId)) {
			return;
		}
		final RootQuery csawQuery = queries.get(queryId).rootQuery;
		
		// check that all snippets are decorated with stems
		for (Snippet snippet : snippets) {
			if (snippet.leftStems.size() != snippet.leftTokens.size() || snippet.rightStems.size() != snippet.rightTokens.size()) {
				throw new IllegalStateException("undecorated snippet");
			}
			logger.warn(snippet.queryId + " " + snippet.docid + " " + snippet.entName);
		}
		
		if (!doFeatures) return;
		
		Integer[] pointers = new Integer[snippets.size()]; // sad, autoboxing
		final float[] scores = new float[snippets.size()];
		FloatOpenHashSet distinctScores = new FloatOpenHashSet();
		for (int sx = 0; sx < pointers.length; ++sx) {
			pointers[sx] = sx;
			final Snippet snippet = snippets.get(sx);
			float sumFeature = 0;
			for (AFeature feature : features) {
				sumFeature += feature.value(csawQuery, snippet);
			}
			scores[sx] = sumFeature;
			distinctScores.add(sumFeature);
			if (snippet.entLabel) {
				MonitorFactory.add("goodScore", "", sumFeature);
			}
			else {
				MonitorFactory.add("badScore", "", sumFeature);
			}
		}
		MonitorFactory.add("numDistinctScores", "", distinctScores.size());

		Arrays.sort(pointers, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return - (int) Math.signum(scores[o1] - scores[o2]); // decreasing order
			}
		});

		for (int sx = 0; sx < pointers.length; ++sx) {
			if (snippets.get(pointers[sx]).entLabel) {
				MonitorFactory.add("firstGoodRank", "", sx);
				break;
			}
		}
	}
}
