package iitb.CSAW.Utils.Sort;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.lang.MutableString;

import java.io.IOException;
import java.util.Comparator;

/**
 * Test harness for {@link BitSortedRunWriter}.
 * @author soumen
 *
 */
public class BitSortedRunTester {
	/**
	 * {@link MutableString} stored in a elaborate fashion in bit streams.
	 */
	@SuppressWarnings("serial")
	static class BitterString extends MutableString implements IBitRecord<BitterString> {
		boolean isNull = true;
		@Override
		public Comparator<BitterString> getComparator() {
			return new Comparator<BitterString>() {
				@Override
				public int compare(BitterString o1, BitterString o2) {
					return o1.compareTo(o2);
				}
			};
		}

		@Override
		public boolean isNull() {
			return isNull;
		}

		@Override
		public void load(InputBitStream ibs) throws IOException {
			length(0);
			final int msn = ibs.readInt(Integer.SIZE);
			for (int msx = 0; msx < msn; ++msx) {
				final int cc = ibs.readInt(Integer.SIZE);
				append((char) cc);
			}
			isNull = false;
		}

		@Override
		public void replace(BitterString ibr) {
			super.replace((MutableString) ibr);
			isNull = false;
		}

		@Override
		public void setNull() {
			isNull = true;
		}

		@Override
		public void store(OutputBitStream obs) throws IOException {
			obs.writeInt(length(), Integer.SIZE);
			for (int msx = 0, msn = length(); msx < msn; ++msx) {
				obs.writeInt(charAt(msx), Integer.SIZE);
			}
			obs.align();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		BitSortedRunWriter<BitterString> bsrw = new BitSortedRunWriter<BitterString>(BitterString.class, 55);
		final String[] words = { "mary", "had", "a", "little", "lamb", "and", "i", "ate", "it", };
		final BitterString bs = new BitterString();
		for (String word : words) {
			bs.replace(word);
			if (!bsrw.append(bs)) {
				break;
			}
		}
		bsrw.sortRun();
		bsrw.printRun(System.out);
	}
}
