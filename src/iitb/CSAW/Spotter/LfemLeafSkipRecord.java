package iitb.CSAW.Spotter;

import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.util.Arrays;

public class LfemLeafSkipRecord implements IRecord {
	public int leaf;
	public int usedBudget;
	public double estimatedTrainCost, estimatedTestCost;
	public final IntArrayList sentFeat = new IntArrayList(), sentBitPlus = new IntArrayList();
	
	public LfemLeafSkipRecord() {
		init();
	}
	
	private void init() {
		leaf = usedBudget = Integer.MIN_VALUE;
		estimatedTrainCost = estimatedTestCost = Double.NaN;
		sentFeat.clear();
		sentBitPlus.clear();
	}
	
	public LfemLeafSkipRecord(int leaf, int ub, double trc, double tec, IntArrayList sentFeat, IntArrayList sentBitPlus) {
		init();
		this.leaf = leaf;
		this.usedBudget = ub;
		this.estimatedTrainCost = trc;
		this.estimatedTestCost = tec;
		this.sentFeat.addAll(sentFeat);
		Arrays.sort(this.sentFeat.elements(), 0, this.sentFeat.size());
		this.sentBitPlus.addAll(sentBitPlus);
		Arrays.sort(this.sentBitPlus.elements(), 0, this.sentBitPlus.size());
	}
	
	@Override
	public <IR extends IRecord> void replace(IR src) {
		init();
		final LfemLeafSkipRecord lls = (LfemLeafSkipRecord) src;
		this.leaf = lls.leaf;
		this.usedBudget = lls.usedBudget;
		this.estimatedTrainCost = lls.estimatedTrainCost;
		this.estimatedTestCost = lls.estimatedTestCost;
		this.sentFeat.addAll(lls.sentFeat);
		this.sentBitPlus.addAll(lls.sentBitPlus);
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		int checksum = 0;
		doi.writeInt(leaf);
		checksum ^= leaf;
		doi.writeInt(usedBudget);
		checksum ^= usedBudget;
		doi.writeDouble(estimatedTrainCost);
		doi.writeDouble(estimatedTestCost);
		doi.writeInt(sentFeat.size());
		checksum ^= sentFeat.size();
		for (int aSentFeat : sentFeat) {
			doi.writeInt(aSentFeat);
			checksum ^= aSentFeat;
		}
		doi.writeInt(sentBitPlus.size());
		checksum ^= sentBitPlus.size();
		for (int aSentBitPlus : sentBitPlus) {
			doi.writeInt(aSentBitPlus);
			checksum ^= aSentBitPlus;
		}
		doi.writeInt(checksum);
	}

	@Override
	public void load(DataInput dii) throws IOException {
		init();
		int checksum = 0;
		leaf = dii.readInt();
		checksum ^= leaf;
		usedBudget = dii.readInt();
		checksum ^= usedBudget;
		estimatedTrainCost = dii.readDouble();
		estimatedTestCost = dii.readDouble();
		final int nSentFeat = dii.readInt();
		checksum ^= nSentFeat;
		for (int sfx = 0; sfx < nSentFeat; ++sfx) {
			final int aSentFeat = dii.readInt();
			checksum ^= aSentFeat;
			sentFeat.add(aSentFeat);
		}
		final int nSentBitPlus = dii.readInt();
		checksum ^= nSentBitPlus;
		for (int sbpx = 0; sbpx < nSentBitPlus; ++sbpx) {
			final int aSentBitPlus = dii.readInt();
			checksum ^= aSentBitPlus;
			sentBitPlus.add(aSentBitPlus);
		}
		if (checksum != dii.readInt()) {
			throw new StreamCorruptedException();
		}
	}
}
