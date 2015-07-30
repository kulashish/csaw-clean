package iitb.CSAW.Spotter;

import iitb.CSAW.Utils.Sort.IRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BayesLidstoneResult implements IRecord {
	int aleaf = -1;
	float alidstone = 0;
	float accuracy = 0;
	float instances = 0;

	@Override
	public void load(DataInput dii) throws IOException {
		aleaf = dii.readInt();
		alidstone = dii.readFloat();
		accuracy = dii.readFloat();
		instances = dii.readFloat();
	}

	@Override
	public <IR extends IRecord> void replace(IR src) {
		final BayesLidstoneResult blr = (BayesLidstoneResult) src;
		aleaf = blr.aleaf;
		alidstone = blr.alidstone;
		accuracy = blr.accuracy;
		instances = blr.instances;
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		doi.writeInt(aleaf);
		doi.writeFloat(alidstone);
		doi.writeFloat(accuracy);
		doi.writeFloat(instances);
	}
}
