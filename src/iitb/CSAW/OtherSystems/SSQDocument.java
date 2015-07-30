package iitb.CSAW.OtherSystems;

import gnu.trove.TIntArrayList;
import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * All token and entity sequence info for SSQ Term-Entity Index should be limited to this one class.
 * @author ganesh
 */
@SuppressWarnings("serial")
public class SSQDocument extends IntArrayList implements IRecord {
	volatile int dictSize, dictBase, postBase, postCursor, prevPos;
	
	public void load(IntArrayList inbuf, int cursor) {
		clear();
		add(inbuf.getInt(cursor++)); // tokId
		add(inbuf.getInt(cursor++)); // docId
		dictSize = inbuf.getInt(cursor++);
		add(dictSize);
		dictBase = cursor; 
		TIntArrayList postingSizesList = new TIntArrayList(dictSize);
		for (int dx = 0; dx < dictSize; ++dx) {
			add(inbuf.getInt(cursor++)); // entLongId in dict
			final int npost = inbuf.getInt(cursor++);
			postingSizesList.add(npost);
			add(npost); // npost for each entity in dict
		}
		postBase = cursor;
		for(int i=0; i<postingSizesList.size(); i++){
			for (int px = 0; px < postingSizesList.get(i); px=px+2) {
				add(inbuf.get(cursor++)); // pgap1
				add(inbuf.get(cursor++)); // pgap2
			}
		}
		postCursor = prevPos = 0;
	}
	
	public void load(DataInput di) throws IOException {
		clear();
		add(di.readInt()); // tokId
		add(di.readInt()); // docId
		dictSize = di.readInt();
		add(dictSize);
		dictBase = size();
		TIntArrayList postingSizesList = new TIntArrayList(dictSize);
		for (int dx = 0; dx < dictSize; ++dx) {
			add(di.readInt()); // entLongId in dict
			final int npost = di.readInt();
			postingSizesList.add(npost);
			add(npost); // npost for each entity in dict
		}
		postBase = size();
		for(int i=0; i<postingSizesList.size(); i++){
			for (int px = 0; px < postingSizesList.get(i); px=px+2) {
				add(di.readInt()); // pgap1
				add(di.readInt()); // pgap2
			}
		}
		postCursor = prevPos = 0;
	}

	public void loadCompressed(InputBitStream ibs, int tokId) throws IOException {

		clear();
		add(tokId); // keep compatible
		final int docId = ibs.readInt(Integer.SIZE); 
		add(docId);
		dictSize = ibs.readGamma(); 
		add(dictSize);
		dictBase = size();
		TIntArrayList postingsSizeList= new TIntArrayList(dictSize);
		for (int dx = 0; dx < dictSize; ++dx) {
			final int entLongId = ibs.readInt(Integer.SIZE);
			add(entLongId);
			final int nPosts = ibs.readGamma();
			postingsSizeList.add(nPosts);
			add(nPosts);
		}
		
		postBase = size();
		for(int i=0; i<postingsSizeList.size(); i++){
			for (int px = 0; px < postingsSizeList.get(i); px=px+2) {
				final int pgap1 = ibs.readGamma();
				add(pgap1);
				final int pgap2 = ibs.readGamma();
				add(pgap2);
			}
		}
		postCursor = prevPos = 0;
	}

	public void storeCompressed(OutputBitStream obs) throws IOException {
		int cursor = 1; 			// note that tokId is not stored
		obs.writeInt(getInt(cursor++), Integer.SIZE); // docId
		System.err.println(get(0));
		final int dictSize = getInt(cursor++); 
		obs.writeGamma(dictSize);
		TIntArrayList postingsSizeList= new TIntArrayList(dictSize);
		for (int dx = 0; dx < dictSize; ++dx) {
			final int entLongId = getInt(cursor++);
			obs.writeInt(entLongId, Integer.SIZE);
			final int nPosts = getInt(cursor++);
			postingsSizeList.add(nPosts);
			obs.writeGamma(nPosts);	
		}
		for(int i=0; i<postingsSizeList.size(); i++){
			for (int px = 0; px < postingsSizeList.get(i); px=px+2) {
				final int pgap1 = getInt(cursor++);
				obs.writeGamma(pgap1);
				final int pgap2 = getInt(cursor++);
				obs.writeGamma(pgap2);
			}
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
		SSQDocument idp = (SSQDocument) src;
		addAll(idp);
		dictSize = idp.dictSize;
		dictBase = idp.dictBase;
		postBase = idp.postBase;
		postCursor = 0;
		prevPos = 0;
	}

	public int tokId() {
		return getInt(0); // INDEX_MAGIC
	}
	
	public int docId() {
		return getInt(1); // INDEX_MAGIC
	}
	
}
