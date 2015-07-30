package iitb.CSAW.EntityRank.Webaroo;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntIterator;
import iitb.CSAW.Spotter.Spot;
import iitb.CSAW.Utils.Sort.ABitReducer;
import iitb.CSAW.Utils.Sort.IBitRecord;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Comparator;

/**
 * <p>Holds information about snippets supporting candidate entities
 * to perform generative query interpretation. Has two embedded classes
 * for different kinds of aggregates.</p>
 * 
 * <p>Note that this is a "sketchy" snippet holding only information about
 * matched query words in aggregate. No positional information is captured.
 * After aggregation, information about individual match witnesses is
 * also lost.</p>
 *  
 * @author soumen
 */
public class SketchySnippet {
	/**
	 * From queryId, ent to numSnippets.
	 */
	public static class QEN implements IBitRecord<QEN>, IBitReducible<QEN> {
		public final MutableString queryId = new MutableString();
		public int ent, numSnippets;

		/**
		 * Lexicographic by {@link #queryId}, {@link #ent}.
		 */
		@Override
		public Comparator<QEN> getComparator() {
			return new Comparator<QEN>() {
				@Override
				public int compare(QEN o1, QEN o2) {
					final int comp = o1.queryId.compareTo(o2.queryId);
					if (comp != 0) { return comp; }
					return o1.ent - o2.ent;
				}
			};
		}
		
		@Override
		public ABitReducer<QEN, QEN> getReducer() {
			return new QenReducer();
		}
		
		@Override
		public boolean isNull() {
			return queryId.length()==0 || ent == Spot.unknownEnt;
		}
		@Override
		public void load(InputBitStream ibs) throws IOException {
			int check = 0;
			SketchySnippet.load(queryId, ibs);
			check ^= queryId.length();
			ent = ibs.readInt(Integer.SIZE);
			check ^= ent;
			numSnippets = ibs.readInt(Integer.SIZE);
			check ^= numSnippets;
			final int oldCheck = ibs.readInt(Integer.SIZE);
			if (check != oldCheck) {
				throw new StreamCorruptedException();
			}
		}
		@Override
		public void replace(QEN ibr) {
			queryId.replace(ibr.queryId);
			ent = ibr.ent;
			numSnippets = ibr.numSnippets;
		}
		@Override
		public void setNull() {
			queryId.length(0);
			ent = Spot.unknownEnt;
			numSnippets = 0;
		}
		@Override
		public void store(OutputBitStream obs) throws IOException {
			int check = 0;
			SketchySnippet.store(queryId, obs);
			check ^= queryId.length();
			obs.writeInt(ent, Integer.SIZE);
			check ^= ent;
			obs.writeInt(numSnippets, Integer.SIZE);
			check ^= numSnippets;
			obs.writeInt(check, Integer.SIZE);
		}
	}
	
	private static class QenReducer extends ABitReducer<QEN, QEN> {
		private final QEN qen = new QEN();
		
		@Override
		public void reset() {
			qen.setNull();
		}
		
		@Override
		public void getResult(QEN outrec) {
			outrec.replace(qen);
		}
		
		@Override
		public int compareKeys(QEN inrec) {
			if (qen.isNull()) { return -1; }
			final int qcomp = qen.queryId.compareTo(inrec.queryId);
			if (qcomp != 0) { return qcomp; }
			return qen.ent - inrec.ent;
		}
		
		@Override
		public void accumulate(QEN inrec) {
			if (qen.isNull()) {
				qen.replace(inrec);
			}
			else {
				if (compareKeys(inrec) != 0) {
					throw new IllegalArgumentException(qen + " :: " + inrec + " = " + compareKeys(inrec));
				}
				qen.numSnippets += inrec.numSnippets;
			}
		}
	}
	
	/**
	 * From queryId, ent to { queryWord -> count }.
	 */
	public static class QEWC implements IBitRecord<QEWC>, IBitReducible<QEWC> {
		public final MutableString queryId = new MutableString();
		public int ent;
		public final TObjectIntHashMap<String> queryWordCount = new TObjectIntHashMap<String>();

		/**
		 * Lexicographic by {@link #queryId}, {@link #ent}.
		 */
		@Override
		public Comparator<QEWC> getComparator() {
			return new Comparator<QEWC>() {
				@Override
				public int compare(QEWC o1, QEWC o2) {
					final int c0 = o1.queryId.compareTo(o2.queryId);
					if (c0 != 0) return c0;
					final int c1 = o1.ent - o2.ent;
					return c1;
				}
			};
		}

		@Override
		public boolean isNull() {
			return queryId.length() == 0 || ent == Spot.unknownEnt;
		}

		@Override
		public void setNull() {
			queryId.length(0);
			ent = Spot.unknownEnt;
			queryWordCount.clear();
		}

		@Override
		public void store(OutputBitStream obs) throws IOException {
			int check = 0;
			SketchySnippet.store(queryId, obs);
			check ^= queryId.length();
			obs.writeInt(ent, Integer.SIZE);
			check ^= ent;
			obs.writeInt(queryWordCount.size(), Integer.SIZE);
			check ^= queryWordCount.size();
			for (TObjectIntIterator<String> qtfx = queryWordCount.iterator(); qtfx.hasNext(); ) {
				qtfx.advance();
				SketchySnippet.store(qtfx.key(), obs);
				check ^= qtfx.key().length();
				obs.writeInt(qtfx.value(), Integer.SIZE);
				check ^= qtfx.value();
			}
			obs.writeInt(check, Integer.SIZE);
		}

		@Override
		public void load(InputBitStream ibs) throws IOException {
			setNull();
			int check = 0;
			SketchySnippet.load(queryId, ibs);
			check ^= queryId.length();
			ent = ibs.readInt(Integer.SIZE);
			check ^= ent;
			final int nqtf = ibs.readInt(Integer.SIZE);
			check ^= nqtf;
			MutableString ms = new MutableString();
			for (int xqtf = 0; xqtf < nqtf; ++xqtf) {
				SketchySnippet.load(ms, ibs);
				check ^=  ms.length();
				final int mc = ibs.readInt(Integer.SIZE);
				check ^= mc;
				queryWordCount.adjustOrPutValue(ms.toString(), mc, mc);
			}
			final int recheck = ibs.readInt(Integer.SIZE);
			if (recheck != check) {
				throw new StreamCorruptedException();
			}
		}

		@Override
		public void replace(QEWC ibr) {
			setNull();
			queryId.replace(ibr.queryId);
			ent = ibr.ent;
			queryWordCount.putAll(ibr.queryWordCount);
		}

		@Override
		public ABitReducer<QEWC, QEWC> getReducer() {
			return new QewcReducer();
		}
	}
	
	private static class QewcReducer extends ABitReducer<QEWC, QEWC> {
		private final QEWC qewc = new QEWC();
		
		@Override
		public void accumulate(QEWC inrec) {
			if (qewc.isNull()) {
				qewc.replace(inrec);
			}
			else {
				if (compareKeys(inrec) != 0) {
					throw new IllegalArgumentException(qewc + " :: " + inrec + " = " + compareKeys(inrec));
				}
				for (TObjectIntIterator<String> qwcx = inrec.queryWordCount.iterator(); qwcx.hasNext(); ) {
					qwcx.advance();
					qewc.queryWordCount.adjustOrPutValue(qwcx.key(), qwcx.value(), qwcx.value());
				}
			}
		}

		@Override
		public int compareKeys(QEWC inrec) {
			if (qewc.isNull()) { return -1; }
			final int qcomp = qewc.queryId.compareTo(inrec.queryId);
			if (qcomp != 0) { return qcomp; }
			return qewc.ent - inrec.ent;
		}

		@Override
		public void getResult(QEWC outrec) {
			outrec.replace(qewc);
		}

		@Override
		public void reset() {
			qewc.setNull();
		}
	}

	private static void store(CharSequence cs, OutputBitStream obs) throws IOException {
		obs.writeInt(cs.length(), Integer.SIZE);
		for (int qix = 0, qin = cs.length(); qix < qin; ++qix) {
			obs.writeInt(cs.charAt(qix), Character.SIZE);
		}
	}

	private static void load(MutableString ms, InputBitStream ibs) throws IOException {
		ms.length(ibs.readInt(Integer.SIZE));
		for (int mx = 0, mn = ms.length(); mx < mn; ++mx) {
			final int mschar = ibs.readInt(Character.SIZE);
			ms.setCharAt(mx, (char) mschar);
		}
	}
}
