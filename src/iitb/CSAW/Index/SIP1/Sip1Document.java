package iitb.CSAW.Index.SIP1;

import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * All record layout and index magic should be limited to this one class, but
 * as of 2010/09/15 a bit of that has smeared into {@link Sip1IndexWriter}.
 * @author soumen
 */
@SuppressWarnings("serial")
public class Sip1Document extends IntArrayList implements IRecord {
	volatile int dictSize, dictBase, nPosts, postBase, postCursor, prevPos;
	
	public void load(IntArrayList inbuf, int cursor) {
		clear();
		add(inbuf.getInt(cursor++)); // catId
		add(inbuf.getInt(cursor++)); // docId
		dictSize = inbuf.getInt(cursor++);
		add(dictSize);
		dictBase = cursor; 
		for (int dx = 0; dx < dictSize; ++dx) {
			add(inbuf.getInt(cursor++)); // entLongId in dict
		}
		nPosts = inbuf.getInt(cursor++);
		add(nPosts);
		postBase = cursor;
		for (int px = 0; px < nPosts; ++px) {
			add(inbuf.get(cursor++)); // pgap
			add(inbuf.get(cursor++)); // entShortId
		}
		postCursor = prevPos = 0;
	}
	
	public void load(DataInput di) throws IOException {
		clear();
		add(di.readInt()); // catId
		add(di.readInt()); // docId
		dictSize = di.readInt();
		add(dictSize);
		dictBase = size();
		for (int dx = 0; dx < dictSize; ++dx) {
			add(di.readInt()); // entLongId in dict
		}
		nPosts = di.readInt();
		add(nPosts);
		postBase = size();
		for (int px = 0; px < nPosts; ++px) {
			add(di.readInt()); // pgap
			add(di.readInt()); // entShortId
		}
		postCursor = prevPos = 0;
	}

	public void loadCompressed(InputBitStream ibs, int catId) throws IOException {
		clear();
		add(catId); // keep compatible
		final int docId = ibs.readInt(Integer.SIZE); 
		add(docId);
		dictSize = ibs.readGamma(); 
		add(dictSize);
		dictBase = size();
		for (int dx = 0; dx < dictSize; ++dx) {
			final int entLongId = ibs.readInt(Integer.SIZE);
			add(entLongId);
		}
		nPosts = ibs.readGamma();
		add(nPosts);
		postBase = size();
		for (int px = 0; px < nPosts; ++px) {
			final int pgap = ibs.readGamma();
			add(pgap);
			final int entShortId = ibs.readGamma();
			add(entShortId);
		}
		postCursor = prevPos = 0;
	}

	public void storeCompressed(OutputBitStream obs) throws IOException {
		int cursor = 1; 			// note that catid is not stored
		obs.writeInt(getInt(cursor++), Integer.SIZE); // docid
		final int dictSize = getInt(cursor++); 
		obs.writeGamma(dictSize);
		for (int dx = 0; dx < dictSize; ++dx) {
			final int entLongId = getInt(cursor++);
			obs.writeInt(entLongId, Integer.SIZE);
		}
		final int nPosts = getInt(cursor++);
		obs.writeGamma(nPosts);
		for (int px = 0; px < nPosts; ++px) {
			final int pgap = getInt(cursor++);
			obs.writeGamma(pgap);
			final int entShortId = getInt(cursor++);
			obs.writeGamma(entShortId);
		}
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		for (int val : this) {
			doi.writeInt(val);
		}
	}
	
	@Override
	public <IR extends IRecord> void replace(IR src) {
		clear();
		Sip1Document idp = (Sip1Document) src;
		addAll(idp);
		dictSize = idp.dictSize;
		dictBase = idp.dictBase;
		nPosts = idp.nPosts;
		postBase = idp.postBase;
		postCursor = 0;
		prevPos = 0;
	}

	public int catId() {
		return getInt(0); // INDEX_MAGIC
	}
	
	public int docId() {
		return getInt(1); // INDEX_MAGIC
	}
	
	public boolean hasNextPosition() {
		return postCursor < nPosts;
	}
	
	/**
	 * Directly report intervals with (long) entity IDs
	 * @param tbws
	 */
	public void getWitnesses(TypeBindingQuery tbq, int docId, ReferenceArrayList<AWitness> tbws) {
		tbws.clear();
		int[] entBeginPos = new int[dictSize], entLastPos = new int[dictSize];
		Arrays.fill(entBeginPos, Integer.MIN_VALUE);
		Arrays.fill(entLastPos, Integer.MIN_VALUE);
		for (int px = 0, prevPos = 0; px < nPosts; ++px) {
			final int pgap = getInt(postBase + 2 *  px), pos = prevPos + pgap;
			final int entShortId = getInt(postBase + 2 * px + 1);
			assert 0 <= entShortId && entShortId < dictSize; 
			if (entLastPos[entShortId] + 1 < pos) {
				getOneWitness(tbq, docId, entShortId, entBeginPos, entLastPos, tbws);
				// start a new ent run
				entBeginPos[entShortId] = entLastPos[entShortId] = pos;
			}
			entLastPos[entShortId] = prevPos = pos;
		}
		// emit any remaining intervals
		for (int entShortId = 0; entShortId < dictSize; ++entShortId) {
			getOneWitness(tbq, docId, entShortId, entBeginPos, entLastPos, tbws);
		}
	}
	
	private void getOneWitness(TypeBindingQuery tbq, int docId, int entShortId, int[] entBeginPos, int[] entLastPos, List<AWitness> tbws) {
		if (0 <= entBeginPos[entShortId] && entBeginPos[entShortId] <= entLastPos[entShortId]) {
			final Interval interval = Interval.valueOf(entBeginPos[entShortId], entLastPos[entShortId]);
			final int entLongId = getInt(dictBase + entShortId);
			final TypeBindingWitness tbw = new TypeBindingWitness(docId, interval, tbq, entLongId);
			tbws.add(tbw);
		}
	}
}
