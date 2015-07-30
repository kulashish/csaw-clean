package iitb.CSAW.Spotter;

import gnu.trove.TObjectIntHashMap;
import gnu.trove.TObjectIntProcedure;
import iitb.CSAW.Utils.Sort.IRecord;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public class ContextRecord implements IRecord, Comparator<ContextRecord> {
	public String entNameOrPhrase = null;
	public final TObjectIntHashMap<String> stemToCount = new TObjectIntHashMap<String>();
	
	public void setNull() {
		entNameOrPhrase = null;
		stemToCount.clear();
	}

	@Override
	public void store(final DataOutput doi) throws IOException {
		doi.writeUTF(entNameOrPhrase);
		doi.writeInt(stemToCount.size());
		stemToCount.forEachEntry(new TObjectIntProcedure<String>() {
			@Override
			public boolean execute(String stem, int count) {
				try {
					doi.writeUTF(stem);
					doi.writeInt(count);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				return true;
			}
		});
	}

	@Override
	public void load(DataInput dii) throws IOException {
		entNameOrPhrase = dii.readUTF();
		final int nStems = dii.readInt();
		stemToCount.clear();
		for (int sx = 0; sx < nStems; ++sx) {
			final String stem = dii.readUTF();
			final int count = dii.readInt();
			stemToCount.adjustOrPutValue(stem, count, count);
		}
	}

	@Override
	public <IR extends IRecord> void replace(IR src) {
		final ContextRecord cr = (ContextRecord) src;
		entNameOrPhrase = cr.entNameOrPhrase;
		stemToCount.clear();
		stemToCount.putAll(cr.stemToCount);
	}

	@Override
	public int compare(ContextRecord o1, ContextRecord o2) {
		return o1.entNameOrPhrase.compareTo(o2.entNameOrPhrase);
	}
	
	@Override
	public String toString() {
		return entNameOrPhrase + "->" + stemToCount;
	}
}