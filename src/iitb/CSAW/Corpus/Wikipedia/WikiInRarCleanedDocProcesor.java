package iitb.CSAW.Corpus.Wikipedia;


import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TCharHashSet;
import gnu.trove.set.hash.THashSet;
import iitb.CSAW.Index.Annotation;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.StringIntBijection;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Interval;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

class WikiInRarCleanedDocProcesor {

	private static THashSet<CharSequence> wordSet = new THashSet<CharSequence>();
	private static String wikiTokensFile;
	private static String wikiEntNamesFile;
	private static String wikiTokensSibj;
	private static String wikiEntNamesSibj;
	private static String wikiAnnotationCountFile;

	WikiInRarCleanedDocProcesor(Config config) {
		String wwb = config.getString("cleanWikipediaWriteBase");
		wikiTokensFile = wwb + "wikiVocab.txt";
		wikiTokensSibj = wwb + "wikiVocab.sibj";
		wikiEntNamesFile = wwb + "wikiEntNames.txt";
		wikiEntNamesSibj = wwb + "wikiEntNames.sibj";
		wikiAnnotationCountFile = wwb + "wikiEntAnnotationCount.txt";
	}

	final String text = "text";
	final String url = "url";
	final String annotations = "annotations";
	final String uri = "uri";
	private String offset = "offset";
	private String surface_form = "surface_form";

	TObjectIntHashMap<String> buildTokensAndEntsFile(File dir)
			throws IOException, JSONException {
		pl.start("Building Tokens and Entities File...");
		TObjectIntHashMap<String> ent2Count = new TObjectIntHashMap<String>();
		BufferedWriter wikiEntWriter = new BufferedWriter(new FileWriter(
				wikiEntNamesFile));
		for (File dx : dir.listFiles()) {
			File[] fileArr = dx.listFiles();
			for (File fx : fileArr) {
				LineIterator lit = new LineIterator(new FastBufferedReader(
						new FileReader(fx)));
				while (lit.hasNext()) {
					String line = lit.next().toString();
					JSONObject jsonObj = new JSONObject(line);
					String page = jsonObj.get(text).toString();
					addToWordSet(page.toCharArray());

					String entName = jsonObj.get(url).toString();
					wikiEntWriter.write(URLDecoder.decode(entName));
					wikiEntWriter.write('\n');

					JSONArray jarr = (JSONArray) jsonObj.get(annotations);

					for (int ix = 0, zx = jarr.length(); ix < zx; ix++) {
						JSONObject ax = (JSONObject) jarr.get(ix);
						String ent = URLDecoder.decode(ax.get(uri).toString());
						if (ent.contains("\n")) {
							continue;
						} else
							ent2Count.adjustOrPutValue(ent, 1, 1);
					}
					pl.update();
				}// end while
			}// end for fx
		}// end for dx
		wikiEntWriter.close();
		pl.done();

		pl.start("Writing Tokens");
		String[] warr = wordSet.toArray(new String[0]);
		java.util.Arrays.sort(warr);
		BufferedWriter wikiTokenWriter = new BufferedWriter(new FileWriter(
				wikiTokensFile));
		for (String s : warr) {
			wikiTokenWriter.write(s);
			wikiTokenWriter.write('\n');
		}
		wikiTokenWriter.close();
		pl.done();

		pl.start("Writing Annotation Count");
		BufferedWriter wikiEntAnnotationCountWriter = new BufferedWriter(
				new FileWriter(wikiAnnotationCountFile));

		for (Object ex : ent2Count.keys()) {
			String eName = (String) ex;
			int val = ent2Count.get(eName);
			assert val > 0;
			wikiEntAnnotationCountWriter.write(eName + "\t" + val + "\n");
		}
		wikiEntAnnotationCountWriter.close();
		pl.done();

		return ent2Count;

	}

	private static char[] acceptedChars = new char[] { 'a', 'b', 'c', 'd', 'e',
			'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
			's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E',
			'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
			'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4',
			'5', '6', '7', '8', '9' };

	private static TCharHashSet reqd = new TCharHashSet(acceptedChars);

	private static void addToWordSet(char[] ca) {
		int ix = 0, lastIx = 0, len = ca.length, wlen;
		for (; ix < len; ix++) {
			if (!reqd.contains(ca[ix])) {
				wlen = ix - lastIx;
				if (wlen != 0)
					wordSet.add(new String(ca, lastIx, wlen));
				lastIx = ix + 1;
			}
		}
		wlen = ix - lastIx;
		if (wlen != 0)
			wordSet.add(new String(ca, lastIx, wlen));
	}

	void getDocAndAnnotations(StringIntBijection tokenDictionary,
			String jsonObjString, MutableString entName, IntArrayList tokens,
			Collection<Annotation> annots) throws JSONException {
		tokens.clear();
		annots.clear();
		entName.setLength(0);

		JSONObject jsonObj = new JSONObject(jsonObjString);
		entName.replace(URLDecoder.decode(jsonObj.getString(url)));

		JSONArray jarr = (JSONArray) jsonObj.get(annotations);
		String page = jsonObj.get(text).toString();

		ArrayList<Integer> beginCharOffset = new ArrayList<Integer>();
		for (int ix = 0, zx = jarr.length(); ix < zx; ix++) {
			JSONObject ax = (JSONObject) jarr.get(ix);
			int left = Integer.parseInt(ax.get(offset).toString());
			beginCharOffset.add(left);
		}

		char c;
		StringBuilder sb = new StringBuilder();
		int tokensWritten = 0, annotationPointer = 0;
		int nextIndex;
		if (beginCharOffset.size() > 0)
			nextIndex = beginCharOffset.get(annotationPointer);
		else
			nextIndex = Integer.MAX_VALUE;

		for (int px = 0; px < page.length(); px++) {
			c = page.charAt(px);
			if (nextIndex == px) {
				JSONObject objx = (JSONObject) jarr.get(annotationPointer);
				String sf = objx.get(surface_form).toString();
				int tempx = countTokens(sf) - 1;
				String eName = URLDecoder.decode(objx.get(uri).toString());
				if (WikiInRarCorpus.entDictionary.containsKey(eName)) {
					annots.add(new Annotation(eName, Interval.valueOf(
							tokensWritten, tokensWritten + tempx), 0, 0));
				} else if (WikiInRarCorpus.redirectEntities.containsKey(eName)) {
					int redirectIndex = WikiInRarCorpus.redirectEntities
							.getInt(eName);
					int eid = WikiInRarCorpus.redirectArr[redirectIndex];
					eName = WikiInRarCorpus.entDictionary.intToString(eid);
					annots.add(new Annotation(eName, Interval.valueOf(
							tokensWritten, tokensWritten + tempx), 0, 0));
				}
				annotationPointer++;
				if (annotationPointer == beginCharOffset.size())
					nextIndex = Integer.MAX_VALUE;
				else
					nextIndex = beginCharOffset.get(annotationPointer);
			}

			if (!reqd.contains(c)) {
				if (sb.length() > 0) {
					int tID = tokenDictionary.getInt(sb.toString());
					assert tID >= 0 : "Token ID negative for String "
							+ sb.toString() + "\n page = " + page;
					tokens.add(tID);
					tokensWritten++;
					sb.delete(0, sb.length());
				}
			} else {
				sb.append(c);
			}
		}
	}

	private int countTokens(String line) {
		char c;
		int numTokens = 1;
		for (int ix = 0; ix < line.length(); ix++) {
			c = line.charAt(ix);
			if (!reqd.contains(c)) {
				numTokens++;
			}
		}
		return numTokens;
	}

	private static StringIntBijection sibj(String file) throws IOException {
		LineIterator lit = new LineIterator(new FastBufferedReader(
				new FileReader(file)));
		ArrayList<String> keys = new ArrayList<String>();
		while (lit.hasNext()) {
			MutableString s = lit.next();
			keys.add(s.toString());
		}
		return new StringIntBijection(keys);
	}

	void buildTokensAndEntsSibj() throws IOException {
		pl.start("Building Dictionaries for Tokens and Entities");
		BinIO.storeObject(sibj(wikiTokensFile), wikiTokensSibj);
		pl.update();
		BinIO.storeObject(sibj(wikiEntNamesFile), wikiEntNamesSibj);
		pl.done();
	}

	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);

	// public static void main(String[] args) throws Exception {
	// final Config config = new Config(args[0], args[1]);
	// WikiInRamCleanedDocProcesor wp = new WikiInRamCleanedDocProcesor(config);
	// if (args[2].equals("buildAll")) {
	// wp.buildTokensAndEntsFile(new File(
	// "/mnt/abc/hrushikesh/data/cleanedWiki/Annotated-WikiExtractor/extracted"));
	// wp.buildTokensAndEntsSibj();
	// } else if (args[2].equals("test")) {
	// BufferedReader br = new BufferedReader(new FileReader(
	// "/mnt/b100/d0/siddhanthjain/Anarchism"));
	// String jsonObjString = br.readLine();
	// wp.getDocAndAnnotations(null, jsonObjString, new MutableString(),
	// new IntArrayList(), new ArrayList<Annotation>());
	// }
	// }

}
