package iitb.CSAW.Index.SIP2;

import gnu.trove.TIntIntHashMap;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.ISipDocument;
import iitb.CSAW.Index.SipDocumentBuilder;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Index.SIP3.Sip3Document;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Utils.RecordDigest;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.io.StreamCorruptedException;
import java.security.DigestException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;

/**
 * Dictionary, long and short entity IDs, the works.
 * In keeping with MG4J style, mutable and dangerous.
 * @author soumen
 */
public class Sip2Document implements ISipDocument<Sip2Document>{
	final Monitor gapMon = MonitorFactory.getMonitor("Sip2Gap", null);
	static final int maxEntOrCatBits = Integer.SIZE;
	
	/** For ent index this is ent ID, for type index this is cat ID. */
	int entOrCatId;
	int docId;
	/** If {@link #dict} is empty, then {@link #entShortIds} is empty too. */
	final IntArrayList dict = new IntArrayList();
	final IntArrayList spanLeft = new IntArrayList();
	/** Right end point of span is {@link #spanLeft} plus {@link #spanSize}. */
	final ByteArrayList spanSize = new ByteArrayList();
	final ShortArrayList entShortIds = new ShortArrayList();
	
	/**
	 * Used to count bits in a record to prepend to the stream written or read
	 * in {@link #store(OutputBitStream)} and {@link #load(InputBitStream)}. 
	 * We do not preserve and copy the bit array but throw it away to /dev/null. 
	 * This wastes time, but otherwise we would need to manage a real byte buffer.
	 * @since 2011/12/07
	 */
	private OutputBitStream nullObs = new OutputBitStream(new NullOutputStream(), (int) FileUtils.ONE_KB * 8);
	
	@Override
	public int docId() { return docId; }

	/**
	 * We write the number of payload bits, then the payload, then align. 
	 * @param obs
	 */
	@Override
	public void store(OutputBitStream obs) throws IOException {
		if (obs != nullObs) {
			nullObs.writtenBits(0);
			store(nullObs); // recursive
			final long payloadBits = nullObs.writtenBits();
			// nullBits accounts for bits written from entOrCatId onward
			// but without the last align padding
			obs.writeLongDelta(payloadBits);
		}
		obs.writeInt(entOrCatId, maxEntOrCatBits);
		obs.writeInt(docId, Integer.SIZE);
		obs.writeGamma(dict.size());
		for (int entLongId : dict) {
			obs.writeInt(entLongId, maxEntOrCatBits);
		}
		obs.writeGamma(spanLeft.size());
		assert spanLeft.size() == spanSize.size();
		final boolean doWriteEntShortIds = !dict.isEmpty();
		for (int prevLeft = 0, sx = 0, sNum = spanLeft.size(); sx < sNum; ++sx) {
			final int sl = spanLeft.getInt(sx);
			assert sl >= prevLeft;
			obs.writeGamma(sl - prevLeft);
			final byte ss = spanSize.getByte(sx);
			obs.writeGamma(ss);
			if (doWriteEntShortIds) {
				final short esi = entShortIds.getShort(sx);
				obs.writeGamma(esi);
			}
			prevLeft = sl;
		}
		if (obs != nullObs) {
			obs.align();
		}
	}
	
	@Override
	public void load(InputBitStream ibs) throws IOException {
		loadHead(ibs);
		loadRest(ibs);
	}
	
	/**
	 * @param ibs
	 * @return remaining payload bits for this record,
	 * required by {@link #skipRest(InputBitStream, long)}
	 * @throws IOException
	 */
	protected long loadHead(InputBitStream ibs) throws IOException {
		final long payloadBits = ibs.readLongDelta();
		setNull();
		entOrCatId = ibs.readInt(maxEntOrCatBits);
		docId = ibs.readInt(Integer.SIZE);
		return payloadBits - maxEntOrCatBits - Integer.SIZE;
	}
	
	protected void skipRest(InputBitStream ibs, long rest) throws IOException {
		ibs.skip(rest);
		ibs.align();
	}
	
	protected void loadRest(InputBitStream ibs) throws IOException {
		dict.clear();
		dict.size(ibs.readGamma());
		for (int dx = 0, dNum = dict.size(); dx < dNum; ++dx) {
			dict.set(dx, ibs.readInt(maxEntOrCatBits));
		}
		spanLeft.clear();
		spanSize.clear();
		entShortIds.clear();
		final int nPosts = ibs.readGamma();
		spanLeft.size(nPosts);
		spanSize.size(nPosts);
		final boolean doReadEntShortIds = !dict.isEmpty();
		if (doReadEntShortIds) {
			entShortIds.size(nPosts);
		}
		for (int prevLeft = 0, px = 0; px < nPosts; ++px) {
			final int sl = prevLeft + ibs.readGamma();
			spanLeft.set(px, sl);
			final int ss = ibs.readGamma();
			assert ss <= Byte.MAX_VALUE;
			spanSize.set(px, (byte) ss);
			if (doReadEntShortIds) {
				final int esi = ibs.readGamma();
				assert 0 <= esi && esi < dict.size();
				entShortIds.set(px, (short) esi);
			}
			prevLeft = sl;
		}
		ibs.align();
	}
	
	/**
	 * Read as fast as possible without loading into any data structure.
	 * @param ibs
	 * @throws IOException
	 */
	public static int skipSlow(InputBitStream ibs) throws IOException {
		int ans = 0;
		final long restBits = ibs.readLongDelta(), readBits = ibs.readBits();
		ans ^= ibs.readInt(maxEntOrCatBits);
		ans ^= ibs.readInt(Integer.SIZE);
		final int nDict = ibs.readGamma();
		ans ^= nDict;
		for (int dx = 0; dx < nDict; ++dx) {
			ans ^= ibs.readInt(maxEntOrCatBits);
		}
		final int nPosts = ibs.readGamma();
		ans ^= nPosts;
		for (int px = 0; px < nPosts; ++px) {
			ans ^= ibs.readGamma();
			ans ^= ibs.readGamma();
			if (nDict > 0) {
				ans ^= ibs.readGamma();
			}
		}
		if (readBits + restBits != ibs.readBits()) {
			throw new StreamCorruptedException((readBits + restBits) + " != " + ibs.readBits());
		}
		ibs.align();
		return ans;
	}
	
	public void init(final int entOrCatId, final int docId) {
		this.entOrCatId = entOrCatId;
		this.docId = docId;
		dict.clear();
		spanLeft.clear();
		spanSize.clear();
		entShortIds.clear();
	}

	public void init(int catId, int docId, IntArrayList entFreqSorter) {
		init(catId, docId);
		dict.addAll(entFreqSorter);
	}

	@Override
	public boolean isNull() {
		return entOrCatId == Integer.MIN_VALUE && docId == Integer.MIN_VALUE;
	}

	@Override
	public void setNull() {
		init(Integer.MIN_VALUE, Integer.MIN_VALUE);
	}

	@Override
	public void replace(Sip2Document o) {
		init(Integer.MIN_VALUE, Integer.MIN_VALUE);
		entOrCatId = o.entOrCatId;
		docId = o.docId;
		dict.addAll(o.dict);
		spanLeft.addAll(o.spanLeft);
		spanSize.addAll(o.spanSize);
		entShortIds.addAll(o.entShortIds);
	}
	
	/**
	 * Exact duplicate of {@link Sip3Document#buildFromElr}
	 * @param sdb
	 * @param cursor
	 * @return
	 */
	@Override
	public int buildFromElr(int docId, SipDocumentBuilder<? extends Sip2Document> sdb, int cursor) {
		setNull();
		this.docId = docId;
		final MutableInt entId = new MutableInt(), left = new MutableInt(), right = new MutableInt();
		while (cursor < sdb.nElr()) {
			sdb.getElr(cursor, entId, left, right);
			if (entOrCatId == Integer.MIN_VALUE) {
				entOrCatId = entId.intValue();
			}
			if (entOrCatId != entId.intValue()) {
				return cursor; // next block for next Sip3Document.ent
			}
			addPostingElement(left.intValue(), right.intValue());
			++cursor;
		}
		return cursor;
	}
	
	/**
	 * Needs several passes over the catId segment starting at input catBegin
	 * @param sdb
	 * @param catBegin
	 * @return
	 */
	@Override
	public int buildFromCelr(int docId, SipDocumentBuilder<? extends Sip2Document> sdb, int catBegin) {
		setNull();
		this.docId = docId;
		final MutableInt nCatId = new MutableInt(), catId = new MutableInt(), entId = new MutableInt(), left = new MutableInt(), right = new MutableInt();
		sdb.getCelr(catBegin, catId, entId, left, right);
		// detect the beginning of next catId run using a prelim pass
		int catEnd = catBegin + 1;
		for (; catEnd < sdb.nCelr(); ++catEnd) {
			sdb.getCelr(catEnd, nCatId, entId, left, right);
			if (!nCatId.equals(catId)) {
				break;
			}
		}
		// collect entity frequencies in another pass
		// TODO reuse this, perhaps by pushing into sdb
		final TIntIntHashMap entToFreq = new TIntIntHashMap();
		for (int cx = catBegin; cx < catEnd; ++cx) {
			sdb.getCelr(cx, catId, entId, left, right);
			entToFreq.adjustOrPutValue(entId.intValue(), 1, 1);
		}
		// TODO reuse this, perhaps by pushing into sdb
		final IntArrayList entFreqSorter = new IntArrayList();
		entToFreq.forEachKey(new TIntProcedure() {
			@Override
			public boolean execute(int entId) {
				entFreqSorter.add(entId);
				return true;
			}
		});
		Sorting.quickSort(entFreqSorter.elements(), 0, entFreqSorter.size(), new IntComparator() {
			@Override
			public int compare(int entId1, int entId2) {
				assert entToFreq.containsKey(entId1);
				assert entToFreq.containsKey(entId2);
				return entToFreq.get(entId2) - entToFreq.get(entId1); // decreasing order
			}
		});
		// prepare dictionary for category
		init(catId.intValue(), docId, entFreqSorter);
		// rescan block and append postings
		for (int cx = catBegin; cx < catEnd; ++cx) {
			sdb.getCelr(cx, catId, entId, left, right);
			addPostingElement(left.intValue(), right.intValue(), entId.intValue());
		}
		return catEnd;
	}

	public void addPostingElement(int sl, int sr) {
		assert entOrCatId != -1;
		assert docId != -1;
		spanLeft.add(sl);
		final int ss = sr - sl;
		if (ss > Byte.MAX_VALUE) {
			throw new IllegalArgumentException("Span [" + sl + "," + sr + "] too large");
		}
		spanSize.add((byte) ss);
	}
	
	public void addPostingElement(int sl, int sr, int entId) {
		addPostingElement(sl, sr);
		final int entShortId = dict.indexOf(entId);
		assert -1 != entShortId;
		assert 0 <= entShortId;
		assert entShortId <= Short.MAX_VALUE;
		entShortIds.add((short) entShortId);
		assert entShortIds.size() == spanLeft.size();
		assert entShortIds.size() == spanSize.size();
	}

	public static class Sip2Comparator implements Comparator<Sip2Document> {
		@Override
		public int compare(Sip2Document s1, Sip2Document s2) {
			
			final int c1 = s1.entOrCatId - s2.entOrCatId;
			if (c1 != 0) {
				return c1;
			}
			return s1.docId - s2.docId;
		}
	}
	
	@Override
	public Sip2Comparator getComparator() {
		return new Sip2Comparator();
	}
	
	public void getWitnesses(TypeBindingQuery tbq, int docId, ReferenceArrayList<? super TypeBindingWitness> tbws) {
		assert this.docId == docId;
		assert !dict.isEmpty();
		tbws.clear();
		for (int px = 0, pNum = spanLeft.size(); px < pNum; ++px) {
			final int sl = spanLeft.getInt(px);
			final int sr = sl + spanSize.getByte(px);
			final Interval interval = Interval.valueOf(sl, sr);
			final int entLongId = dict.getInt(entShortIds.getShort(px));
			final TypeBindingWitness tbw = new TypeBindingWitness(docId, interval, tbq, entLongId);
			tbws.add(tbw);
		}
	}

	public void getWitnesses(EntityLiteralQuery elq, int docId, ReferenceArrayList<? super EntityLiteralWitness> elws) {
		assert this.docId == docId;
		assert dict.isEmpty();
		elws.clear();
		for (int px = 0, pNum = spanLeft.size(); px < pNum; ++px) {
			final int sl = spanLeft.getInt(px);
			final int sr = sl + spanSize.getByte(px);
			final Interval interval = Interval.valueOf(sl, sr);
			final EntityLiteralWitness elw = new EntityLiteralWitness(docId, interval, elq);
			elws.add(elw);
		}
	}
	
	public Interval getSpan(int mx) {
		return Interval.valueOf(spanLeft.getInt(mx), spanLeft.getInt(mx) + spanSize.getByte(mx));
	}
	
	@Override
	public void checkSum(RecordDigest pd) throws DigestException {
		pd.appendInt(entOrCatId);
		pd.appendInt(docId);
		pd.appendInt(dict.size());
		for (int entLongId : dict) {
			pd.appendInt(entLongId);
		}
		final boolean hasShortIds = !dict.isEmpty();
		pd.appendInt(spanLeft.size());
		for (int sx = 0, sn = spanLeft.size(); sx < sn; ++sx) {
			pd.appendInt(spanLeft.getInt(sx));
			pd.appendByte(spanSize.getByte(sx));
			if (hasShortIds) {
				pd.appendShort(entShortIds.getShort(sx));
			}
		}
		pd.endRecord();
	}

	@Override
	public int entOrCatId() {
		return entOrCatId;
	}

	@Override
	public int nPosts() {
		return spanLeft.size();
	}
	
	/**
	 * For testing.
	 * @author sasik
	 */
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("t=" + entOrCatId + " d=" + docId);
		sb.append(" ne:" + dict.size() + " ");
		assert spanLeft.size() == spanSize.size() : "Type=" + entOrCatId + " Doc=" + docId + " spanLeftSize=" + spanLeft.size() + " spanSize size=" + spanSize.size();
		sb.append(" ns:" + spanSize.size()); 
		// Sip3 is written in the order of entities. Do the same here also. 
		int[] entIdsArr = dict.toIntArray();
		Arrays.sort(entIdsArr);
		assert dict.size() == entIdsArr.length;
		int spotSno = 0; 
		for(int ent : entIdsArr) {
			final int entIndex = dict.indexOf(ent);
			for(int i=0, n=spanSize.size(); i<n; i++) {
				if(entShortIds.get(i) == entIndex) {
					sb.append(" sn=" + spotSno + " l=" + spanLeft.get(i) + " s=" + spanSize.get(i) + " e=" + dict.get(entShortIds.get(i)));
					++spotSno;
				}
			}
		}
		return sb.toString();
	}

	/**
	 * For verification of on-disk space.
	 * @param nPosts
	 * @param otherBits
	 * @param dictBits
	 * @param gapBits
	 * @throws IOException 
	 */
	void estimatePayloadBitsOnDisk(OutputBitStream nullObs, MutableLong nPosts, MutableLong otherBits, MutableLong dictBits, MutableLong gapBits) throws IOException {
		assert (otherBits.longValue() + dictBits.longValue() + gapBits.longValue()) % Byte.SIZE == 0;
		long sumBits = 0;
		otherBits.add(maxEntOrCatBits); // entOrCatid
		sumBits += maxEntOrCatBits;
		otherBits.add(Integer.SIZE); // docId
		sumBits += Integer.SIZE;
		dictBits.add(nullObs.writeGamma(dict.size())); // dict size
		sumBits += nullObs.writeGamma(dict.size());
		if (!dict.isEmpty()) {
			final long dictBitsAdd = ((long) dict.size()) * ((long) maxEntOrCatBits);
			dictBits.add(dictBitsAdd); // dict itself
			sumBits += dictBitsAdd;
		}
		final boolean areThereEntShortIds = !dict.isEmpty();
		gapBits.add(nullObs.writeGamma(spanLeft.size())); // posting.size
		sumBits += nullObs.writeGamma(spanLeft.size());
		nPosts.add(spanLeft.size());
		for (int prevLeft = 0, sx = 0, sNum = spanLeft.size(); sx < sNum; ++sx) {
			final int sl = spanLeft.getInt(sx);
			assert sl >= prevLeft;
			final int gap = sl - prevLeft;
			gapMon.add(gap);
			gapBits.add(nullObs.writeGamma(gap)); // lgap
			sumBits += nullObs.writeGamma(gap);
			final byte ss = spanSize.getByte(sx);
			gapBits.add(nullObs.writeGamma(ss)); // rgap
			sumBits += nullObs.writeGamma(ss);
			if (areThereEntShortIds) {
				final short esi = entShortIds.getShort(sx);
				dictBits.add(nullObs.writeGamma(esi)); // short ent ID
				sumBits += nullObs.writeGamma(esi);
			}
			prevLeft = sl;
		}
		final long padBits = ((sumBits + Byte.SIZE - 1) / Byte.SIZE) * Byte.SIZE - sumBits;
		otherBits.add(padBits);
		assert (otherBits.longValue() + dictBits.longValue() + gapBits.longValue()) % Byte.SIZE == 0;
	}

	void estimateDeployedBits(int prevDoc, OutputBitStream nullObs, MutableLong nPosts, MutableLong otherBits, MutableLong dictBits, MutableLong gapBits) throws IOException {
		// no need to write entOrCatid
		// write dgap
		assert prevDoc < docId : "prevDoc=" + prevDoc + " doc=" + docId;
		final int dgap = docId - prevDoc - 1;
		final long dgapBits = nullObs.writeGamma(dgap);
		otherBits.add(dgapBits);
		// dict
		dictBits.add(nullObs.writeGamma(dict.size())); // dict size
		if (!dict.isEmpty()) {
			final long dictBitsAdd = ((long) dict.size()) * ((long) maxEntOrCatBits);
			dictBits.add(dictBitsAdd); // dict itself
		}
		// number of posts
		nPosts.add(spanLeft.size());
		final long nPostBits = nullObs.writeGamma(spanLeft.size());
		otherBits.add(nPostBits);
		final boolean areThereEntShortIds = !dict.isEmpty();
		// postings
		for (int prevLeft = 0, sx = 0, sNum = spanLeft.size(); sx < sNum; ++sx) {
			final int sl = spanLeft.getInt(sx);
			assert sl >= prevLeft;
			final int gap = sl - prevLeft;
			gapMon.add(gap);
			gapBits.add(nullObs.writeGamma(gap)); // lgap
			final byte ss = spanSize.getByte(sx);
			gapBits.add(nullObs.writeGamma(ss)); // rgap
			if (areThereEntShortIds) {
				final short esi = entShortIds.getShort(sx);
				final int shortEntBits = nullObs.writeGamma(esi);
				dictBits.add(shortEntBits);
			}
			prevLeft = sl;
		}
		// no doc level padding
	}

	void estimateDeployedBitsLongEnt(int prevDoc, OutputBitStream nullObs, MutableLong nPosts, MutableLong otherBits, MutableLong longEntBits, MutableLong gapBits) throws IOException {
		// no need to write entOrCatid
		// write dgap
		assert prevDoc < docId;
		final int dgap = docId - prevDoc - 1;
		final long dgapBits = nullObs.writeGamma(dgap);
		otherBits.add(dgapBits);
		// no dict
		final boolean areThereEntShortIds = !dict.isEmpty();
		// number of posts
		nPosts.add(spanLeft.size());
		final long nPostBits = nullObs.writeGamma(spanLeft.size());
		otherBits.add(nPostBits);
		// postings
		for (int prevLeft = 0, sx = 0, sNum = spanLeft.size(); sx < sNum; ++sx) {
			final int sl = spanLeft.getInt(sx);
			assert sl >= prevLeft;
			final int gap = sl - prevLeft;
			gapBits.add(nullObs.writeGamma(gap)); // lgap
			final byte ss = spanSize.getByte(sx);
			gapBits.add(nullObs.writeGamma(ss)); // rgap
			if (areThereEntShortIds) {
				longEntBits.add(maxEntOrCatBits); // fixed size long ent
			}
			prevLeft = sl;
		}
		// no padding
	}
	
	void estimateDeployedBitsLucenePayload(int prevDoc, OutputBitStream nullObs, MutableLong nPosts, MutableLong otherBits, MutableLong dictBits, MutableLong gapBits) throws IOException {
		// no need to write entOrCatid
		// write dgap
		assert prevDoc < docId : "prevDoc=" + prevDoc + " doc=" + docId;
		final int dgap = docId - prevDoc - 1;
		final long dgapBits = nullObs.writeGamma(dgap);
		otherBits.add(dgapBits);
		// dict
		dictBits.add(nullObs.writeGamma(dict.size())); // dict size
		if (!dict.isEmpty()) {
			final long dictBitsAdd = ((long) dict.size()) * ((long) maxEntOrCatBits);
			dictBits.add(dictBitsAdd); // dict itself
		}
		// number of posts
		nPosts.add(spanLeft.size());
		final long nPostBits = nullObs.writeGamma(spanLeft.size());
		otherBits.add(nPostBits);
		final boolean areThereEntShortIds = !dict.isEmpty();
		// postings
		for (int prevLeft = 0, sx = 0, sNum = spanLeft.size(); sx < sNum; ++sx) {
			final int sl = spanLeft.getInt(sx);
			assert sl >= prevLeft;
			final int gap = sl - prevLeft;
			gapMon.add(gap);
			gapBits.add(nullObs.writeGamma(gap)); // lgap
			final byte ss = spanSize.getByte(sx);
			gapBits.add(nullObs.writeGamma(ss)); // rgap
			if (areThereEntShortIds) {
				final short esi = entShortIds.getShort(sx);
				final int shortEntBits = nullObs.writeGamma(esi);
				// pad to next byte
				final int paddedShortEntBits = ((shortEntBits + Byte.SIZE - 1) / Byte.SIZE) * Byte.SIZE;
				dictBits.add(paddedShortEntBits);
			}
			prevLeft = sl;
		}
		// no doc level padding
	}
}
