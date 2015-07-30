package iitb.CSAW.Utils;

import it.unimi.dsi.lang.MutableString;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class IntToStringSet implements Serializable {
	private static final long serialVersionUID = 1L;
	final int[] setBaseArray;
	final int[] charBaseArray;
	final MutableString payload;

	public IntToStringSet(List<String> srcArrays[]) {
		setBaseArray = new int[srcArrays.length + 1];
		setBaseArray[0] = 0;
		int numCharBases = 1;
		int numChars = 0;
		for (final List<String> srcArray : srcArrays) {
			numCharBases += srcArray.size();
			for (String src : srcArray) {
				numChars += src.length();
			}
		}
		charBaseArray = new int[numCharBases];
		charBaseArray[0] = 0;
		payload = new MutableString(numChars + 2);
		payload.length(0);
		for (int sax = 0, setCursor = 0, charCursor = 0; sax < srcArrays.length; ++sax) {
			final List<String> srcArray = srcArrays[sax];
			for (int sx = 0; sx < srcArray.size(); ++sx) {
				final String src = srcArray.get(sx);
				payload.append(src);
				charCursor += src.length();
				charBaseArray[++setCursor] = charCursor;
			}
			setBaseArray[sax + 1] = setCursor;
		}

//		payload.compact();
	}
	
	public int numKeys() {
		return setBaseArray.length - 1;
	}
	
	public int keyToNumValues(int key) {
		return setBaseArray[key + 1] - setBaseArray[key]; 
	}
	
	public void keyToValue(int key, int ofs, MutableString ans) {
		final int setBase = setBaseArray[key] + ofs;
		final int charBaseBegin = charBaseArray[setBase];
		final int charBaseEnd = charBaseArray[setBase + 1];
		ans.replace(payload.subSequence(charBaseBegin, charBaseEnd));
	}
	
	@SuppressWarnings("unchecked")
	public static final void main(String[] args) throws Exception {
		List<String> e0 = Arrays.asList("e0s0", "e0s1");
		List<String> e1 = Arrays.asList();
		List<String> e2 = Arrays.asList("", "e2s1", "e2s2" );
		List<String>[] src = new List[] { e0, e1, e2 };
		IntToStringSet i2ss = new IntToStringSet(src);
		MutableString ms = new MutableString(128);
		for (int key = 0; key < i2ss.numKeys(); ++key) {
			final int numValues = i2ss.keyToNumValues(key);
			System.out.println("key=" + key + " maps to " + numValues + " strings");
			for (int ofs = 0; ofs < numValues; ++ofs) {
				i2ss.keyToValue(key, ofs, ms);
				System.out.println("\t_" + ms + "_");
			}
		}
	}
}
