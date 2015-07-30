package iitb.CSAW.EntityRank.Wikipedia;

import iitb.CSAW.EntityRank.Feature.AFeature;
import iitb.CSAW.Search.BaseQueryProcessor;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.mutable.MutableInt;

import com.jamonapi.MonitorFactory;

/**
 * Base class for scanning {@link Snippet}, coalescing adjacent mentions
 * of the same entity, and presenting all snippets for one query in one
 * tidy block to a handler method which has to be implemented by subclasses.
 * @author soumen
 */
@Deprecated
public abstract class SnippetScannerBase extends BaseQueryProcessor {
	final ArrayList<AFeature> features;
	
	@SuppressWarnings("unchecked")
	public SnippetScannerBase(Config conf) throws Exception {
		super(conf);
		this.features = new ArrayList<AFeature>();
		/*
		 * Example of how a "registry" of features can be created.
		 */
		List<Object> featureNames = conf.getList(iitb.CSAW.EntityRank.PropertyKeys.snippetFeaturesKey);
		for (String featureName : (List<String>)(Object)featureNames) {
			features.add((AFeature) Class.forName(featureName).getConstructor(Config.class).newInstance(conf));
		}
	}
	
	public void scanAll(ISnippetHandler snippetHandler) throws IOException {
		File sortedSnippetFile = new File(config.getString(iitb.CSAW.EntityRank.PropertyKeys.sortedSnippetFileKey));
		DataInputStream dis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(sortedSnippetFile)));
		String lastQueryId = null;
		ArrayList<Snippet> lastQuerySnippets = new ArrayList<Snippet>();
		try {
			for (Snippet snippet = new Snippet(); ;) {
				snippet.load(dis);
				if (lastQueryId != null && !lastQueryId.equals(snippet.queryId)) {
					scanOneQuery(lastQuerySnippets, snippetHandler);
					lastQuerySnippets.clear();
				}
				Snippet rememberSnippet = new Snippet();
				rememberSnippet.replace(snippet);
				lastQuerySnippets.add(rememberSnippet);
				lastQueryId = rememberSnippet.queryId;
			} // for snippet
		}
		catch (EOFException eofx) {
			logger.debug("end of file");
		}
		if (!lastQuerySnippets.isEmpty()) {
			scanOneQuery(lastQuerySnippets, snippetHandler);
		}
		dis.close();
		logger.info(MonitorFactory.getMonitor("coalesce", ""));
	}
	
	void scanOneQuery(ArrayList<Snippet> snippets, ISnippetHandler snippetHandler) {
		coalesceSnippetsOfOneQuery(snippets);
		for (Snippet snippet : snippets) {
			attachStems(snippet.leftTokens, snippet.leftStems);
			attachStems(snippet.rightTokens, snippet.rightStems);
		}
		snippetHandler.handleOneQuery(snippets);
	}
	
	void attachStems(ObjectArrayList<String> inTokens, ObjectArrayList<MutableString> outStems) {
		outStems.clear();
		for (String inToken : inTokens) {
			MutableString msToken = new MutableString(inToken);
			termProcessor.processTerm(msToken);
			outStems.add(msToken);
		}
	}
	
	/**
	 * Coalesce snippets with same entity in adjacent positions.
	 * @param snippets
	 */
	void coalesceSnippetsOfOneQuery(ArrayList<Snippet> snippets) {
		if (snippets.isEmpty()) {
			return;
		}
		ArrayList<Snippet> ans = new ArrayList<Snippet>();
		Snippet pending = null;
		for (Snippet snippet : snippets) {
			if (pending != null && !pending.queryId.equals(snippet.queryId)) {
				throw new IllegalArgumentException("Snippets belong to two different queries " + pending.queryId + " and " + snippet.queryId);
			}
			if (pending != null) {
				if (isContiguous(pending, snippet)) {
					coalese(pending, snippet);
				}
				else {
					ans.add(pending);
					pending = snippet;
				}
			}
			else {
				pending = snippet;
			}
		}
		if (pending != null) {
			ans.add(pending);
		}
		final double reduction = (double) ans.size() / (double) snippets.size();
		MonitorFactory.add("coalesce", "", reduction);
		snippets.clear();
		snippets.addAll(ans);
	}
	
	boolean isContiguous(Snippet left, Snippet right) {
		return left.queryId.equals(right.queryId) && left.docid == right.docid && left.entName.equals(right.entName) && left.entLabel == right.entLabel && left.entEndOffset + 1 == right.entBeginOffset;
	}
	
	/**
	 * Extend {@link Snippet#entEndOffset left.entEndOffset}, 
	 * {@link Snippet#leftTokens left.leftTokens} and
	 * {@link Snippet#rightTokens left.rightTokens} using right.
	 * @param left in/out
	 * @param right in
	 */
	void coalese(Snippet left, Snippet right) {
		if (!isContiguous(left, right)) {
			throw new IllegalArgumentException("Left and right snippets are not contiguous");
		}
		left.entEndOffset = right.entEndOffset;
		left.rightTokens.clear();
		left.rightTokens.addAll(right.rightTokens);
		left.matchOffsets.addAll(right.matchOffsets);
	}

	void collectStatistics(ArrayList<Snippet> snippets, MutableInt nGoodSnippets, MutableInt nBadSnippets, HashSet<String> posEnts, HashSet<String> negEnts) {
		MonitorFactory.add("numSnip", "", snippets.size());
		for (Snippet snippet : snippets) {
			if (snippet.entLabel) {
				nGoodSnippets.increment();
				posEnts.add(snippet.entName);
			}
			else {
				nBadSnippets.increment();
				negEnts.add(snippet.entName);
			}
		}
	}
}