package iitb.CSAW.EntityRank;

import java.io.IOException;
import java.util.Comparator;

import iitb.CSAW.Utils.Sort.IBitRecord;
import it.unimi.dsi.fastutil.ints.Int2FloatArrayMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.util.Interval;

/**
 * Currently designed for single type binding snippets for single entity search.
 * @author soumen
 */
public class SnippetFeatureVector extends Int2FloatArrayMap implements IBitRecord<SnippetFeatureVector> {
	private static final long serialVersionUID = 1L;
	public final MutableString qid = new MutableString();
	public int ent = -1;
	public int docid = -1;
	public Interval witnessSpan = null;
	// rest is label and feature vector, not part of key
	public boolean entLabel = false;

	@Override
	public Comparator<SnippetFeatureVector> getComparator() {
		return new Comparator<SnippetFeatureVector>() {
			@Override
			public int compare(SnippetFeatureVector o1, SnippetFeatureVector o2) {
				final int qidCmp = o1.qid.compareTo(o2.qid);
				if (qidCmp != 0) return qidCmp;
				final int entCmp = o1.ent - o2.ent;
				if (entCmp != 0) return entCmp;
				final int docCmp = o1.docid - o2.docid;
				if (docCmp != 0) return docCmp;
				if (o1.witnessSpan.right < o2.witnessSpan.left) return -1;
				if (o1.witnessSpan.left > o2.witnessSpan.right) return 1;
				return 0;
			}
		};
	}

	@Override
	public void setNull() {
		qid.length(0);
		ent = docid = -1;
		witnessSpan = null;
		clear(); // feature vector part
	}

	@Override
	public boolean isNull() {
		return qid.length() == 0;
	}
	
	public void construct(CharSequence qid, int ent, int docid, Interval witnessSpan, boolean entLabel) {
		setNull();
		this.qid.replace(qid);
		this.ent = ent;
		this.docid = docid;
		this.witnessSpan = witnessSpan;
		this.entLabel = entLabel;
	}
	
	@Override
	public void replace(SnippetFeatureVector ibr) {
		construct(ibr.qid, ibr.ent, ibr.docid, ibr.witnessSpan, ibr.entLabel);
		putAll(ibr);
	}
	
	/* Nothing smart but may do the job. */

	@Override
	public void store(OutputBitStream obs) throws IOException {
		obs.writeGamma(qid.length());
		for (int qx = 0, qn = qid.length(); qx < qn; ++qx) {
			obs.writeGamma(qid.charAt(qx));
		}
		obs.writeInt(ent, Integer.SIZE);
		obs.writeInt(docid, Integer.SIZE);
		obs.writeInt(witnessSpan.left, Integer.SIZE);
		obs.writeGamma(witnessSpan.right - witnessSpan.left);
		obs.writeGamma(entLabel? 1 : 0);
		obs.writeGamma(size());
		for (int fkey : keySet()) {
			obs.writeGamma(fkey);
			obs.writeInt(Float.floatToIntBits(get(fkey)), Integer.SIZE);
		}
		obs.writeInt(qid.length() ^ ent ^ docid ^ size(), Integer.SIZE); // check bits
		obs.align();
	}

	@Override
	public void load(InputBitStream ibs) throws IOException {
		setNull();
		final int qn = ibs.readGamma();
		for (int qx = 0; qx < qn; ++qx) {
			qid.append((char) ibs.readGamma());
		}
		ent = ibs.readInt(Integer.SIZE);
		docid = ibs.readInt(Integer.SIZE);
		final int wsl = ibs.readInt(Integer.SIZE);
		final int wsr = wsl + ibs.readGamma();
		witnessSpan = Interval.valueOf(wsl, wsr);
		entLabel = ibs.readGamma() > 0;
		final int fn = ibs.readGamma();
		for (int fx = 0; fx < fn; ++fx) {
			final int fkey = ibs.readGamma();
			final float fval = Float.intBitsToFloat(ibs.readInt(Integer.SIZE));
			put(fkey, fval);
		}
		final int check = ibs.readInt(Integer.SIZE);
		ibs.align();
		if (check != (qn ^ ent ^ docid ^ fn)) {
			throw new IllegalStateException("Input stream corrupted.");
		}
	}
}
