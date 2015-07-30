package iitb.CSAW.Catalog.YAGO;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.lang.MutableString;

import java.util.Arrays;

public class YagoCatalogTester {
	/**
	 * @param args [0]=config [1]=opcode [2...]=args
	 */
	public static void main(String[] args) throws Exception {
		Config props = new Config(args[0], "/dev/null");
		final ACatalog yc = YagoCatalog.getInstance(props);
		if (args[1].equals("synset")) {
			mainSynset(yc, args);
		}
		else if (args[1].equals("hypen")) {
			// "Robert_Duvall", "Sherlock_Holmes", "The_Big_Over_Easy"
			mainHypen(yc, args);
		}
		else if (args[1].equals("ents")) {
			mainEnts(yc, args);
		}
	}
	
	static void mainEnts(final ACatalog yc, String[] args) throws Exception {
		for (String catName : Arrays.copyOfRange(args, 2, args.length)) {
			final int catid = yc.catNameToCatID(catName);
			if (catid < 0) {
				System.out.println("type {" + catName + "} not found");
				continue;
			}
			System.out.println(catName);
			TIntHashSet entIDs = new TIntHashSet();
			yc.entsReachableFromCat(catid, entIDs);
			entIDs.forEach(new TIntProcedure() {
				@Override
				public boolean execute(int entID) {
					String entName = yc.entIDToEntName(entID);
					System.out.println("\t" + entName);
					return true;
				}
			});
		}
	}

	static void mainHypen(final ACatalog yc, String[] args) throws Exception {
		for (String entName : Arrays.copyOfRange(args, 2, args.length)) {
			int entid = yc.entNameToEntID(entName);
			if (entid >= 0) {
				TIntHashSet catids = new TIntHashSet();
				yc.catsReachableFromEnt(entid, catids);
				catids.forEach(new TIntProcedure() {
					@Override
					public boolean execute(int catid) {
						System.out.println(yc.catIDToCatName(catid));
						return true;
					}
				});
			}
			System.out.println();
		}
	}
	
	static void mainSynset(ACatalog yc, String[] args) throws Exception {
		for (String canon : Arrays.copyOfRange(args, 2, args.length)) {
			System.out.println("looking up {" + canon + "}");
			int id;
			if ((id = yc.entNameToEntID(canon)) >= 0) {
				final int numLemmas = yc.entIDToNumLemmas(id);
				System.out.println("entity {" + canon + "} has " + numLemmas + " lemmas");	
				MutableString lemma = new MutableString();
				for (int lx = 0; lx < numLemmas; ++lx) {
					yc.entIDToLemma(id, lx, lemma);
					System.out.println("\t" + lemma);
				}
			}
			else if ((id = yc.catNameToCatID(canon)) >= 0) {
				final int numLemmas = yc.catIDToNumLemmas(id);
				System.out.println("category {" + canon + "} has " + numLemmas + " lemmas");	
				MutableString lemma = new MutableString();
				for (int lx = 0; lx < numLemmas; ++lx) {
					yc.catIDToLemma(id, lx, lemma);
					System.out.println("\t" + lemma);
				}
			}
			else {
				System.out.println("{" + canon + "} not found");
			}
		}
	}
}
