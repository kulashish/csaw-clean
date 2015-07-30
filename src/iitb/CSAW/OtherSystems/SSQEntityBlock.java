package iitb.CSAW.OtherSystems;

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
public class SSQEntityBlock extends IntArrayList implements IRecord {
	volatile int postSize;
	
	public synchronized int getPostSize() {
		return postSize;
	}

	public synchronized void setPostSize(int postSize) {
		this.postSize = postSize;
		set(2,postSize);
	}

	public void load(IntArrayList inbuf, int cursor, int tokId, int entId, int nposts) {
		clear();
		add(tokId); // tokId
		add(entId); // entId
		this.postSize = nposts;
		add(postSize);
		for (int dx = 0; dx < postSize; dx++) {
			add(inbuf.getInt(cursor++)); 
		}
	}
	
	public void load(DataInput di) throws IOException {
		clear();
		add(di.readInt()); // tokId
		add(di.readInt()); // entId
		postSize = di.readInt();
		add(postSize);
		for (int dx = 0; dx < postSize; dx++) {
			add(di.readInt());
		}
	}

	public void loadCompressed(InputBitStream ibs, int tokId) throws IOException {
		clear();
		add(tokId); // keep compatible
		final int entId = ibs.readGamma();//ibs.readInt(Integer.SIZE); 
		add(entId);
		postSize = ibs.readGamma(); 
		add(postSize);
		for (int dx = 0; dx < postSize; dx++) {
			final int pos = ibs.readGamma(); 
			add(pos);
		}
	}

	public void storeCompressed(OutputBitStream obs, int npost) throws IOException {
		int cursor = 1; 			// note that tokId is not stored
		obs.writeGamma(getInt(cursor++));//writeInt(getInt(cursor++), Integer.SIZE); // entId
		//System.err.println(get(0));
		/*postSize = */getInt(cursor++); 
		obs.writeGamma(npost);
		for(int i=0; i<postSize; i++){
			final int pos = getInt(cursor++);
			obs.writeGamma(pos);
		}
	}
	
	public void storeCompressed(OutputBitStream obs) throws IOException {
		int cursor = 1; 			// note that tokId is not stored
		obs.writeGamma(getInt(cursor++));//writeInt(getInt(cursor++), Integer.SIZE); // entId
		//System.err.println(get(0));
		/*postSize = */getInt(cursor++); 
		obs.writeGamma(postSize);
		for(int i=0; i<postSize; i++){
			final int pos = getInt(cursor++);
			obs.writeGamma(pos);
		}
	}

	public void storeCompressedPostings(OutputBitStream obs) throws IOException {
		int cursor = 3; 
		for(int i=0; i<postSize; i++){
			final int pos = getInt(cursor++);
			obs.writeGamma(pos);
		}
	}
	
	@Override
	public SSQEntityBlock clone(){
		SSQEntityBlock newBlock = new SSQEntityBlock();
		newBlock.addAll(this);
		newBlock.postSize = this.postSize;
		return newBlock;
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
		SSQEntityBlock idp = (SSQEntityBlock) src;
		addAll(idp);
		postSize = idp.postSize;
	}

	public int tokId() {
		return getInt(0); // INDEX_MAGIC
	}
	
	public int entId() {
		return getInt(1); // INDEX_MAGIC
	}
	
}
