package iitb.CSAW.Spotter;

import gnu.trove.HashFunctions;
import gnu.trove.TIntFloatHashMap;
import gnu.trove.TIntFloatIterator;
import gnu.trove.TIntIntIterator;
import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Collections;

import org.apache.commons.lang.mutable.MutableInt;

/**
 * Mutable and very dangerous, per MG4J culture.
 * @author soumen
 */
public class ContextFeatureVector extends TIntFloatHashMap implements IRecord {
	int leaf = Integer.MIN_VALUE, ent = Integer.MIN_VALUE;
	
	public void setNull() {
		leaf = ent = Integer.MIN_VALUE;
		clear();
	}
	
	/**
	 * Raw counts, suitable for multinomial naive Bayes.
	 * @param crc input context
	 * @param tcr for mapping to feature IDs
	 * @return
	 */
	public static void makeCountFeatureVector(ContextRecordCompact crc, TokenCountsReader tcr, ContextFeatureVector cfv) {
		cfv.setNull();
		cfv.leaf = crc.trieLeaf;
		cfv.ent = crc.entId;
		for (TIntIntIterator nearX = crc.nearTermIdToCount.iterator(); nearX.hasNext(); ) {
			nearX.advance();
			cfv.put(nearX.key(), nearX.value());
		}
		// Note knowledge of feature space design here.
		final int fbase = tcr.vocabularySize();
		for (TIntIntIterator salX = crc.salientTermIdToCount.iterator(); salX.hasNext(); ) {
			salX.advance();
			cfv.put(fbase + salX.key(), salX.value());
		}
	}
	
	public void ellOneNormalize() {
		float sum_ = 0;
		for (TIntFloatIterator cfvx = iterator(); cfvx.hasNext(); ) {
			cfvx.advance();
			final float val_ = cfvx.value();
			sum_ += val_;
		}
		for (TIntFloatIterator cfvx = iterator(); cfvx.hasNext(); ) {
			cfvx.advance();
			final float newVal = cfvx.value() / sum_;
			if (Float.isInfinite(newVal) || Float.isNaN(newVal)) {
				throw new ArithmeticException("Cannot normalize zero feature vector.");
			}
			cfvx.setValue(newVal);
		}
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("CFV[L" + leaf + ",E" + ent + "]{");
		IntArrayList keys = new IntArrayList(keys());
		Collections.sort(keys);
		for (int key : keys) {
			sb.append(key);
			sb.append("=");
			sb.append(get(key));
			sb.append(",");
		}
		sb.append("}");
		return sb.toString();
	}

	@Override
	public void load(DataInput dii) throws IOException {
		leaf = dii.readInt();
		ent = dii.readInt();
		final int fill = dii.readInt();
		clear();
		for (int fx = 0; fx < fill; ++fx) {
			put(dii.readInt(), dii.readFloat());
		}
		if ((leaf ^ ent ^ fill) != dii.readInt()) {
			throw new StreamCorruptedException();
		}
	}
	
	public static void load(DataInput dii, MutableInt mleaf, MutableInt ment, IntArrayList fvk, FloatArrayList fvv) throws IOException {
		mleaf.setValue(dii.readInt());
		ment.setValue(dii.readInt());
		final int fill = dii.readInt();
		fvk.clear();
		fvv.clear();
		for (int fx = 0; fx < fill; ++fx) {
			fvk.add(dii.readInt());
			fvv.add(dii.readFloat());
		}
		if ((mleaf.intValue() ^ ment.intValue() ^ fill) != dii.readInt()) {
			throw new StreamCorruptedException();
		}
	}

	@Override
	public <IR extends IRecord> void replace(IR src) {
		final ContextFeatureVector ocfv = (ContextFeatureVector) src;
		leaf = ocfv.leaf;
		ent = ocfv.ent;
		clear();
		putAll(ocfv);
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		doi.writeInt(leaf);
		doi.writeInt(ent);
		doi.writeInt(size());
		for (TIntFloatIterator tx = iterator(); tx.hasNext(); ) {
			tx.advance();
			doi.writeInt(tx.key());
			doi.writeFloat(tx.value());
		}
		doi.writeInt(leaf ^ ent ^ size());
	}
	
	@Override
	public int hashCode() {
		int ans = 0;
		ans ^= HashFunctions.hash(leaf);
		ans ^= HashFunctions.hash(ent);
		ans ^= HashFunctions.hash(size());
		for (TIntFloatIterator cfvx = iterator(); cfvx.hasNext(); ) {
			cfvx.advance();
			ans ^= HashFunctions.hash(cfvx.key());
			ans ^= HashFunctions.hash(cfvx.value());
		}
		return ans;
	}
}
