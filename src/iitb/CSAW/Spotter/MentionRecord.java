package iitb.CSAW.Spotter;

import iitb.CSAW.Utils.Sort.IRecord;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Each record contains
 * <ul>
 * <li>An entity name and</li>
 * <li>A mention phrase flattened into one string.  We generally assume
 * the words in the phrase are stemmed, but nothing here enforces that.</li>
 * <li>A count of the number of times the entity and phrase pair were
 * encountered in a reference corpus. This is filled in meaningfully only
 * after a merge.</li>
 * </ul>
 * 
 * @author soumen
 */
public class MentionRecord implements IRecord {
	static final byte magic = 0x51;
	public final MutableString entName = new MutableString();
	final MutableString buffer = new MutableString();
	final IntArrayList tokenEnds = new IntArrayList();
	public int count = 0;
	
	public void clear() {
		entName.length(0);
		buffer.length(0);
		tokenEnds.clear();
		count = 0;
	}
	
	public void append(CharSequence mentionToken) {
		buffer.append(mentionToken);
		tokenEnds.add(buffer.length());
	}

	@Override
	public <IR extends IRecord> void replace(IR o) {
		final MentionRecord mention = (MentionRecord) o;
		entName.replace(mention.entName);
		buffer.replace(mention.buffer);
		tokenEnds.clear();
		tokenEnds.addAll(mention.tokenEnds);
		count = mention.count;
	}

	@Override
	public void load(DataInput dii) throws IOException {
		if (dii.readByte() != magic) {
			throw new IOException("data stream corrupted");
		}
		clear();
		entName.readSelfDelimUTF8(dii);
		final int nTokens = dii.readInt();
		for (int tx = 0; tx < nTokens; ++tx) {
			final int nChars = dii.readInt();
			for (int cx = 0; cx < nChars; ++cx) {
				buffer.append(dii.readChar());
			}
			tokenEnds.add(buffer.length());
		}
		count = dii.readInt();
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		doi.writeByte(magic);
		entName.writeSelfDelimUTF8(doi);
		final int nTokens = size();
		doi.writeInt(nTokens);
		for (int tx = 0; tx < nTokens; ++tx) {
			final MutableString token = token(tx);
			final int nChars = token.length();
			doi.writeInt(nChars);
			for (int cx = 0; cx < nChars; ++cx) {
				doi.writeChar(token.charAt(cx));
			}
		}
		doi.writeInt(count);
	}

	public MutableString token(int tokenOffset) {
		final int cBegin = tokenOffset == 0? 0: tokenEnds.getInt(tokenOffset-1);
		return buffer.substring(cBegin, tokenEnds.getInt(tokenOffset));
	}
	
	public int size() {
		return tokenEnds.size();
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(count + " X ");
		sb.append(entName + " --- ");
		for (int mx = 0; mx < size(); ++mx) {
			if (mx > 0) {
				sb.append('|');
			}
			sb.append(token(mx));
		}
		return sb.toString();
	}
	
	/* A bunch of comparators */
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MentionRecord)) return false;
		final MentionRecord omr = (MentionRecord) obj;
		return entName.equals(omr.entName) && buffer.equals(omr.buffer) && tokenEnds.equals(omr.tokenEnds);
	}

	@Override
	public int hashCode() {
		return entName.hashCode() ^ buffer.hashCode();
	}
	
	public static class MentionEntityComparator implements Comparator<MentionRecord> {
		final MentionComparator mc = new MentionComparator();
		final EntityComparator ec = new EntityComparator();
		
		@Override
		public int compare(MentionRecord o1, MentionRecord o2) {
			final int mr = mc.compare(o1, o2);
			if (mr != 0) return mr;
			return ec.compare(o1, o2);
		}
	}
	
	public static class EntityComparator implements java.util.Comparator<MentionRecord> {
		@Override
		public int compare(MentionRecord o1, MentionRecord o2) {
			return o1.entName.compareTo(o2.entName);
		}
		
	}
	
	public static class MentionComparator implements java.util.Comparator<MentionRecord> {
		@Override
		public int compare(MentionRecord la, MentionRecord lb) {
			final int na = la.tokenEnds.size(), nb = lb.tokenEnds.size(), nc = Math.min(na, nb);
			for(int lx = 0; lx < nc; ++lx) {
				final int cElem = la.token(lx).compareTo(lb.token(lx));
				if (cElem != 0) {
					return cElem;
				}
			}
			return na - nb;
		}
	}
}
