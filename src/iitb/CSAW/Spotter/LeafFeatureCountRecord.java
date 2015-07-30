package iitb.CSAW.Spotter;

import java.io.IOException;
import java.util.Comparator;

import iitb.CSAW.Utils.Sort.IBitRecord;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/**
 * Used to collect leaf, feature statistics from the payload corpus.
 * @author soumen
 */
public class LeafFeatureCountRecord implements IBitRecord<LeafFeatureCountRecord> {
	public int leaf, feat;
	public long count;
	
	public boolean keyEquals(LeafFeatureCountRecord lfcr) {
		return leaf == lfcr.leaf && feat == lfcr.feat;
	}
	
	@Override
	public Comparator<LeafFeatureCountRecord> getComparator() {
		return new Comparator<LeafFeatureCountRecord>() {
			@Override
			public int compare(LeafFeatureCountRecord o1, LeafFeatureCountRecord o2) {
				final int leafDiff = o1.leaf - o2.leaf;
				if (leafDiff != 0) return leafDiff;
				return o1.feat - o2.feat;
			}
		};
	}

	@Override
	public boolean isNull() {
		return count == Long.MIN_VALUE;
	}

	@Override
	public void load(InputBitStream ibs) throws IOException {
		leaf = ibs.readInt(Integer.SIZE);
		feat = ibs.readInt(Integer.SIZE);
		count = ibs.readLongDelta();
	}

	@Override
	public void replace(LeafFeatureCountRecord lfcr) {
		leaf = lfcr.leaf;
		feat = lfcr.feat;
		count = lfcr.count;
	}

	@Override
	public void setNull() {
		leaf = feat = 0;
		count = Long.MIN_VALUE;
	}

	@Override
	public void store(OutputBitStream obs) throws IOException {
		obs.writeInt(leaf, Integer.SIZE);
		obs.writeInt(feat, Integer.SIZE);
		obs.writeLongDelta(count);
	}
}
