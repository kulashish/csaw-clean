package iitb.CSAW.Spotter;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;

public class LeafTrainTaskPool {

	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2]=opcode
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		MentionTrie trie = MentionTrie.getInstance(conf);
		LeafTrainTaskPool lttp = new LeafTrainTaskPool(conf);
		lttp.initialize(trie.getNumLeaves());
		lttp.close();
	}

	final Config conf;
	final File poolDir;
	final RandomAccessFile poolRaf;
	static final byte FREE=0, TAKEN=1, DONE=2;
	final int recSize = 2; // two bytes, hostStripe and status
	
	LeafTrainTaskPool(Config conf) throws FileNotFoundException {
		this.conf = conf;
		this.poolDir = new File(conf.getString(getClass().getSimpleName()));
		poolRaf = new RandomAccessFile(new File(poolDir, "Pool.dat"), "rws");
	}

	/**
	 * Should call offline before starting workers.
	 * @param nLeaf
	 * @throws IOException 
	 */
	void initialize(int nLeaf) throws IOException {
		poolRaf.setLength(0);
		byte[] buf = new byte[recSize * nLeaf];
		poolRaf.write(buf);
	}
	
	synchronized void takeLeaves(int upto, IntList oleaves, int hostStripe) throws IOException {
		oleaves.clear();
		poolRaf.seek(0);
		for (long recBase = 0; (recBase = poolRaf.getFilePointer()) < poolRaf.length() && oleaves.size() < upto; ) {
			FileLock poolLock = poolRaf.getChannel().lock(recBase, recSize, false);
			final byte status = poolRaf.readByte();
			/* oldHost = */ poolRaf.readByte();
			if (status != DONE) {
				oleaves.add((int) recBase / recSize);
				poolRaf.seek(recBase);
				poolRaf.writeByte(TAKEN);
				poolRaf.writeByte(hostStripe);
			}
			poolLock.release();
		}
	}
	
	synchronized void doneLeaves(IntList dl) throws IOException {
		for (int leaf : dl) {
			doneLeaf(leaf);
		}
	}
	
	synchronized void doneLeaf(int dl) throws IOException {
		final long recBase = dl * recSize;
		poolRaf.seek(recBase);
		FileLock poolLock = poolRaf.getChannel().lock(recBase, recSize, false);
		poolRaf.writeByte(DONE);
		poolLock.release();
	}
	
	void close() throws IOException {
		poolRaf.close();
	}
}
