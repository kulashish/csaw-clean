package iitb.CSAW.Utils;

import gnu.trove.TIntArrayList;

import java.io.Serializable;

public class IntToIntSet implements Serializable {
	private static final long serialVersionUID = 1L;
	final int[] base;
	final int[] payload;

	public IntToIntSet(TIntArrayList[] srcArrays) {
		base = new int[srcArrays.length+1]; // internally shift up by 1
		base[0] = 0;
		int numValues = 0;
		for (final TIntArrayList srcArray : srcArrays) {
			numValues += srcArray.size();
		}
		payload = new int[numValues];
		for (int sax = 0, cursor = 0; sax < srcArrays.length; ++sax) {
			final TIntArrayList srcArray = srcArrays[sax];
			for (int sx = 0; sx < srcArray.size(); ++sx) {
				payload[cursor++] = srcArray.getQuick(sx);
			}
			base[sax + 1] = cursor;
		}
	}

	public int numKeys() {
		return base.length - 1; 
	}

	public int keyToNumValues(int key) {
		return base[key + 1] - base[key];
	}

	int keyToValue(int key, int ofs) {
		return payload[base[key] + ofs];
	}
	
	public static final void main(String[] args) throws Exception {
		TIntArrayList e0 = new TIntArrayList(new int[] { 0, 1 });
		TIntArrayList e1 = new TIntArrayList(new int[] {  });
		TIntArrayList e2 = new TIntArrayList(new int[] { 20, 21, 22 });
		TIntArrayList[] src = new TIntArrayList[] { e0, e1, e2 };
		IntToIntSet i2is = new IntToIntSet(src);
		for (int key = 0; key < i2is.numKeys(); ++key) {
			final int numValues = i2is.keyToNumValues(key);
			System.out.println("key=" + key + " maps to " + numValues + " ints");
			for (int ofs = 0; ofs < numValues; ++ofs) {
				System.out.println("\t" + i2is.keyToValue(key, ofs));
			}
		}
	}
}
