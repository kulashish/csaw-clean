package iitb.CSAW.EntityRank.Wikipedia;

import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Equivalent of iitb.qa.AnswerContext in IR4QA. 
 * This is a detailed snippet in the sense that all left and right
 * context tokens are preserved herein. A random seek per snippet
 * to obtain all these context tokens may not be practical at Web scale.
 *  
 * @author soumen
 */
public class Snippet implements Serializable, IRecord {
	private static final long serialVersionUID = -8451889647410555240L;
	private static final byte versionMagic = 1;
	
	// members are freely writable by outsiders
	
	public String queryId;
	public String atypeName;
	// TODO dial down to int, what's the point
	public long docid;
	/** Offsets are token offsets in full original document. */
	public int entBeginOffset, entEndOffset;
	public String entName;
	public boolean entLabel;
	/** 
	 * Slot 0 is unused. 1 onwards goes left in {@link #leftTokens} and
	 * right in {@link rightTokens}. <b>Note</b> that tokens are not stemmed.
	 */
	public final ObjectArrayList<String> leftTokens = new ObjectArrayList<String>(), rightTokens = new ObjectArrayList<String>();
	/**
	 * Scratch storage to store stems before passing to feature functions. 
	 */
	public transient final ObjectArrayList<MutableString> leftStems = new ObjectArrayList<MutableString>(), rightStems = new ObjectArrayList<MutableString>();
	/**
	 * Offsets are token offsets in full original document. 
	 * To access {@link #leftTokens} or {@link #rightTokens},
	 * need to do some index arithmetic. At present, all match positions
	 * are collapsed into this one set. May diversify if needed.
	 */
	public final IntSet matchOffsets = new IntOpenHashSet();

	@Override
	public <IR extends IRecord> void replace(IR o) {
		Snippet other = (Snippet) o;
		queryId = other.queryId;
		atypeName = other.atypeName;
		docid = other.docid;
		entBeginOffset = other.entBeginOffset;
		entEndOffset = other.entEndOffset;
		entName = other.entName;
		entLabel = other.entLabel;
		leftTokens.clear();
		leftTokens.addAll(other.leftTokens);
		rightTokens.clear();
		rightTokens.addAll(other.rightTokens);
		matchOffsets.clear();
		matchOffsets.addAll(other.matchOffsets);
	}

	@Override
	public void load(DataInput dii) throws IOException {
		if (dii.readLong() != serialVersionUID) {
			throw new IOException("data stream corrupted: bad leading serialVersionUID");
		}
		if (dii.readByte() != versionMagic) {
			throw new IOException("data stream corrupted: bad versionMagic");
		}
		queryId = dii.readUTF();
		atypeName = dii.readUTF();
		docid = dii.readLong();
		entBeginOffset = dii.readInt();
		entEndOffset = dii.readInt();
		entName = dii.readUTF();
		entLabel = dii.readBoolean();
		read(dii, leftTokens);
		read(dii, rightTokens);
		read(dii, matchOffsets);
		if (dii.readLong() != serialVersionUID) {
			throw new IOException("data stream corrupted: bad trailing serialVersionUID");
		}
	}
	
	@Override
	public void store(DataOutput doi) throws IOException {
		doi.writeLong(serialVersionUID);
		doi.writeByte(versionMagic);
		doi.writeUTF(queryId);
		doi.writeUTF(atypeName);
		doi.writeLong(docid);
		doi.writeInt(entBeginOffset);
		doi.writeInt(entEndOffset);
		doi.writeUTF(entName);
		doi.writeBoolean(entLabel);
		write(doi, leftTokens);
		write(doi, rightTokens);
		write(doi, matchOffsets);
		doi.writeLong(serialVersionUID);
	}

	void write(DataOutput doi, IntArrayList ial) throws IOException {
		doi.writeInt(ial.size());
		for (int ii : ial) {
			doi.writeInt(ii);
		}
	}
	
	void read(DataInput dii, IntArrayList ial) throws IOException {
		ial.clear();
		final int nElems = dii.readInt();
		for (int ex = 0; ex < nElems; ++ex) {
			ial.add(dii.readInt());
		}
	}
	
	void write(DataOutput doi, List<String> sal) throws IOException {
		doi.writeInt(sal.size());
		for (String ss : sal) {
			doi.writeUTF(ss);
		}
	}
	
	void read(DataInput dii, List<String> sal) throws IOException {
		sal.clear();
		final int nElems = dii.readInt();
		for (int ex = 0; ex < nElems; ++ex) {
			sal.add(dii.readUTF());
		}
	}
	
	void write(DataOutput doi, IntSet iset) throws IOException {
		doi.writeInt(iset.size());
		for (int val : iset) {
			doi.writeInt(val);
		}
	}
	
	void read(DataInput dii, IntSet iset) throws IOException {
		iset.clear();
		final int nElems = dii.readInt();
		for (int ex = 0; ex < nElems; ++ex) {
			iset.add(dii.readInt());
		}
	}
	
	public String toString() {
		MutableString ms = new MutableString();
		ms.append(queryId + "[" + atypeName + "] " + docid + "[" + entName + "] " + entLabel + "\n");
		for (int tx = leftTokens.size()-1; tx > 0; --tx) {
			final int offsetInDoc = entBeginOffset - tx;
			if (matchOffsets.contains(offsetInDoc)) {
				ms.append('*');
			}
			ms.append(leftTokens.get(tx) + " ");
		}
		ms.append("__" + entName + "__ ");
		for (int tx = 1; tx < rightTokens.size(); ++tx) {
			final int offsetInDoc = entEndOffset + tx;
			if (matchOffsets.contains(offsetInDoc)) {
				ms.append('*');
			}
			ms.append(rightTokens.get(tx) + " ");
		}
		return ms.toString();
	}
}
