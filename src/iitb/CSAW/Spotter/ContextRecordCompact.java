package iitb.CSAW.Spotter;

import gnu.trove.HashFunctions;
import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntIntProcedure;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.util.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.lang.NotImplementedException;

/**
 * Unlike {@link ContextRecord}, here we retain {@link #trieLeaf}, and we turn
 * stems in {@link #termIdToCount} into integer term IDs using
 * {@link TokenCountsReader}.
 * @since 2011/03/10
 * @author soumen
 */
public class ContextRecordCompact implements IRecord, Comparator<ContextRecordCompact>{
	public long docId;
	public Interval mentionSpan;
	public int trieLeaf = Integer.MIN_VALUE, entId = Integer.MIN_VALUE;
//	public final TIntIntHashMap termIdToCount = new TIntIntHashMap();
	
	/** Bag of words near mention */
	public final TIntIntHashMap nearTermIdToCount = new TIntIntHashMap();
	/** Bag of salient words from the whole document containing mention */
	public final TIntIntHashMap salientTermIdToCount = new TIntIntHashMap();
	
	public void init() {
		docId = Long.MIN_VALUE;
		mentionSpan = null;
		trieLeaf = entId = -1;
		nearTermIdToCount.clear();
		salientTermIdToCount.clear();
	}

	@Override
	public <IR extends IRecord> void replace(IR _src) {
		final ContextRecordCompact src = (ContextRecordCompact) _src;
		docId = src.docId;
		mentionSpan = src.mentionSpan;
		trieLeaf = src.trieLeaf;
		entId = src.entId;
		nearTermIdToCount.clear();
		nearTermIdToCount.putAll(src.nearTermIdToCount);
		salientTermIdToCount.clear();
		salientTermIdToCount.putAll(src.salientTermIdToCount);
	}

	private void store(final DataOutput doi, TIntIntHashMap tid2cnt) throws IOException {
		doi.writeInt(tid2cnt.size());
		tid2cnt.forEachEntry(new TIntIntProcedure() {
			@Override
			public boolean execute(int termId, int count) {
				try {
					doi.writeInt(termId);
					doi.writeInt(count);
				} catch (IOException ex) {
					throw new RuntimeException(ex);
				}
				return true;
			}
		});
		doi.writeInt(trieLeaf ^ entId); // cheap "checksum"
	}
	
	@Override
	public void store(final DataOutput doi) throws IOException {
		doi.writeLong(docId);
		doi.writeInt(mentionSpan.left);
		doi.writeInt(mentionSpan.right);
		doi.writeInt(trieLeaf);
		doi.writeInt(entId);
		store(doi, nearTermIdToCount);
		store(doi, salientTermIdToCount);
	}
	
	void load(final DataInput dii, TIntIntHashMap tid2cnt) throws IOException {
		tid2cnt.clear();
		final int bagSize = dii.readInt();
		for (int bx = 0; bx < bagSize; ++bx) {
			final int termId = dii.readInt();
			final int count = dii.readInt();
			tid2cnt.adjustOrPutValue(termId, count, count);
		}
		final int check = dii.readInt();
		if ((trieLeaf ^ entId) != check) {
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void load(DataInput dii) throws IOException {
		docId = dii.readLong();
		mentionSpan = Interval.valueOf(dii.readInt(), dii.readInt());
		trieLeaf = dii.readInt();
		entId = dii.readInt();
		load(dii, nearTermIdToCount);
		load(dii, salientTermIdToCount);
	}
	
	public static class HashedComparator implements Comparator<ContextRecordCompact> {
		@Override
		public int compare(ContextRecordCompact o1, ContextRecordCompact o2) {
			return o1.hashCode() - o2.hashCode();
		}
	}

	@Override
	public int compare(ContextRecordCompact o1, ContextRecordCompact o2) {
		final int c1 = o1.trieLeaf - o2.trieLeaf;
		if (c1 != 0) return c1;
		return o1.entId - o2.entId;
	}

	@Override
	public boolean equals(Object obj) {
		throw new NotImplementedException();
	}

	@Override
	public int hashCode() {
		return HashFunctions.hash(docId) ^ HashFunctions.hash(mentionSpan.left) ^ HashFunctions.hash(mentionSpan.right) ^ HashFunctions.hash(trieLeaf);
	}

	@Override
	public String toString() {
		return "CRC_L" + trieLeaf + "_E" + entId + "_D" + docId + mentionSpan; 
	}
	
	private void toStringSorted(TIntIntHashMap fmap, StringBuilder sb) {
		sb.append("{");
		IntArrayList keys = new IntArrayList(fmap.keys());
		Collections.sort(keys);
		for (int key : keys) {
			sb.append(key);
			sb.append("=");
			sb.append(fmap.get(key));
			sb.append(",");
		}
		sb.append("}");
	}
	
	public String toDetailString() {
		StringBuilder sb = new StringBuilder();
		sb.append(toString());
		sb.append(",NEAR=");
		toStringSorted(nearTermIdToCount, sb);
		sb.append(",SALIENT=");
		toStringSorted(salientTermIdToCount, sb);
		return sb.toString();
	}
}
