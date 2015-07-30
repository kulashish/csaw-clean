package iitb.CSAW.Utils;

import gnu.trove.TLongLongHashMap;

import org.apache.commons.lang.mutable.MutableInt;

/**
 * Uses a {@link TLongLongHashMap} to implement a hash map from two ints as
 * key to two ints as value. <b>Not thread-safe.</b>
 * @author soumen
 */
public class IntIntToIntIntHashMap extends TLongLongHashMap {
	private static final long serialVersionUID = 4195017138536506131L;

	public IntIntToIntIntHashMap() {
		super();
	}

	public IntIntToIntIntHashMap(int capacity) {
		super(capacity);
	}
	
	/** Makes this class not thread-safe */
	private final LongIntInt liik = new LongIntInt(), liiv = new LongIntInt();
	
	public boolean adjustValues(int ku, int kl, int au, int al, MutableInt ovu, MutableInt ovl) {
		liik.write(ku, kl);
		if (!containsKey(liik.lv)) {
			liiv.write(au, al);
			put(liik.lv, liiv.lv);
			return false;
		}
		final long vv = get(liik.lv);
		liiv.write(vv);
		liiv.write(liiv.iv1 + au, liiv.iv0 + al);
		final long ov = put(liik.lv, liiv.lv);
		if (ovu != null && ovl != null) {
			liiv.write(ov);
			ovu.setValue(liiv.iv1);
			ovl.setValue(liiv.iv0);
		}
		return true;
	}
}
