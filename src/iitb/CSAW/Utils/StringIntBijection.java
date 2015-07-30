package iitb.CSAW.Utils;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.sux4j.mph.MWHCFunction;
import it.unimi.dsi.util.ShiftAddXorSignedStringMap;

import java.io.IOException;
import java.util.Arrays;

import com.martiansoftware.jsap.JSAPException;

public class StringIntBijection extends ShiftAddXorSignedStringMap {
	private static final long serialVersionUID = 7530734181797236597L;
	final int ofs[];
	final StringBuilder sb;

	public StringIntBijection(Iterable<? extends CharSequence> keys) throws IOException {
		super(keys.iterator(), new MWHCFunction<CharSequence>(keys, TransformationStrategies.utf16()));
		int sbLength = 10, nKeys = 0;
		for (CharSequence key : keys) {
			sbLength += key.length();
			++nKeys;
		}
		sb = new StringBuilder(sbLength);
		ofs = new int[nKeys];
		for (CharSequence key : keys) {
			final int val = (int) getLong(key);
			ofs[val] = key.length();
			sb.append(key);
		}
		for (int ox = 1; ox < ofs.length; ++ox) {
			ofs[ox] += ofs[ox-1];
		}
		defaultReturnValue(-1);
	}
	
	public int getInt(CharSequence key) {
		return (int) getLong(key);
	}
	
	public String intToString(int id) {
		final int beg = (id == 0)? 0 : ofs[id-1];
		final int end = ofs[id];
		return sb.substring(beg, end);
	}
	
	public void verify() {
		for (int kid = 0; kid < size(); ++kid) {
			String key = intToString(kid);
			int kod = getInt(key);
			if (kid != kod) {
				throw new IllegalStateException("kid=" + kid + " kod=" + kod + " key=" + key);
			}
		}
	}
	
	public static void main(String[] args) throws NoSuchMethodException, IOException, JSAPException, ClassNotFoundException {
		String[] keys = { "mary", "had", "a", "little", "lamb" };
		StringIntBijection sib = new StringIntBijection(Arrays.asList(keys));
		sib.verify();
	}
}
