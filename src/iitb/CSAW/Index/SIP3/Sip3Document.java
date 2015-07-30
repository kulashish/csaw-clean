package iitb.CSAW.Index.SIP3;

import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Index.AWitness;
import iitb.CSAW.Index.EntityLiteralWitness;
import iitb.CSAW.Index.ISipDocument;
import iitb.CSAW.Index.SipDocumentBuilder;
import iitb.CSAW.Index.TypeBindingWitness;
import iitb.CSAW.Index.SIP2.Sip2Document;
import iitb.CSAW.Query.EntityLiteralQuery;
import iitb.CSAW.Query.TypeBindingQuery;
import iitb.CSAW.Utils.RecordDigest;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.util.Interval;

import java.io.IOException;
import java.security.DigestException;
import java.util.Comparator;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang.mutable.MutableLong;

import com.jamonapi.Monitor;
import com.jamonapi.MonitorFactory;

/**
 * No inlined dictionary, instead, finish off postings entity by entity.
 * In keeping with MG4J style, mutable and dangerous.
 * @author soumen
 * @since 2011/05/07
 */
public class Sip3Document implements ISipDocument<Sip3Document>{
	final Monitor gapMon = MonitorFactory.getMonitor("Sip3Gap", null);
	static final int maxEntOrCatBits = Integer.SIZE;
	int entOrCatId = Integer.MIN_VALUE;
	int docId = Integer.MIN_VALUE;
	/** "Long" entity IDs. Empty in case of the {@link Field#ent} index. */
	final IntArrayList entIds = new IntArrayList();
	/** These are <em>excluded end</em> offsets into {@link #spanLeft} and {@link #spanSize}.
	 * Ganged with {@link #entIds}. */
	final IntArrayList entOfs = new IntArrayList();
	final IntArrayList spanLeft = new IntArrayList();
	/** Ganged with {@link #spanLeft} */
	final ByteArrayList spanSize = new ByteArrayList();
	
	@Override
	public Sip3Comparator getComparator() {
		return new Sip3Comparator();
	}
	
	static class Sip3Comparator implements Comparator<Sip3Document> {
		@Override
		public int compare(Sip3Document s1, Sip3Document s2) {
			final int c1 = s1.entOrCatId - s2.entOrCatId;
			if (c1 != 0) return c1;
			return s1.docId - s2.docId;
		}
	}

	@Override
	public boolean isNull() {
		return entOrCatId == Integer.MIN_VALUE && docId == Integer.MIN_VALUE;
	}

	@Override
	public void setNull() {
		entOrCatId = docId = Integer.MIN_VALUE;
		entIds.clear();
		entOfs.clear();
		spanLeft.clear();
		spanSize.clear();
	}
	
	@Override
	public void replace(Sip3Document o) {
		setNull();
		entOrCatId = o.entOrCatId;
		docId = o.docId;
		entIds.addAll(o.entIds);
		entOfs.addAll(o.entOfs);
		spanLeft.addAll(o.spanLeft);
		spanSize.addAll(o.spanSize);
	}
	
	@Override
	public void store(OutputBitStream obs) throws IOException {
		checkSanity();
		obs.writeInt(entOrCatId, maxEntOrCatBits);
		obs.writeInt(docId, Integer.SIZE);
		final int en = entIds.size();
		assert en == entOfs.size() : "Number of entities " + en + " != " + entOfs.size();
		obs.writeGamma(en);
		for (int ex = 0; ex < en; ++ex) {
			obs.writeInt(entIds.getInt(ex), maxEntOrCatBits);
		}
		// no need to write num ents again
		for (int ex = 0, prevOfs = 0; ex < en; ++ex) {
			final int entOf = entOfs.getInt(ex);
			final int gap = entOf-prevOfs;
			assert gap >= 0;
			obs.writeGamma(gap);
			prevOfs = entOf;
		}
		if (en == 0) {
			// this is an entity posting, so we write out the number of spans
			obs.writeGamma(spanLeft.size());
			for (int prevLeft = 0, sx = 0, sn = spanLeft.size(); sx < sn; ++sx) {
				final int sl = spanLeft.getInt(sx);
				assert sl >= prevLeft;
				obs.writeGamma(sl - prevLeft);
				final byte ss = spanSize.getByte(sx);
				obs.writeGamma(ss);
				prevLeft = sl;
			}
		}
		else {
			// type posting; we write a zero to be consistent while loading
			obs.writeGamma(0);
			for (int ex = 0; ex < en; ++ex) { // each entity block
				final int sbeg = (ex == 0)? 0 : entOfs.getInt(ex-1);
				final int send = entOfs.getInt(ex);
				for (int sx = sbeg, prevLeft = 0; sx < send; ++sx) { // each posting in entity block
					final int sl = spanLeft.getInt(sx);
					assert sl >= prevLeft;
					obs.writeGamma(sl - prevLeft);
					final byte ss = spanSize.getByte(sx);
					obs.writeGamma(ss);
					prevLeft = sl;
				}
			}
		}
		obs.align();
	}

	@Override
	public void load(InputBitStream ibs) throws IOException {
		setNull();
		entOrCatId = ibs.readInt(maxEntOrCatBits);
		docId = ibs.readInt(Integer.SIZE);
		final int en = ibs.readGamma(); 
		entIds.size(en);
		for (int ex = 0; ex < en; ++ex) {
			entIds.set(ex, ibs.readInt(maxEntOrCatBits));
		}
		entOfs.size(en);
		for (int ex = 0, prevOfs = 0; ex < en; ++ex) {
			final int gap = ibs.readGamma();
			entOfs.set(ex, prevOfs + gap);
			prevOfs += gap;
		}
		final int sn = ibs.readGamma();
		/*
		 * If en == 0, this is an entity posting, so we just read the number of
		 * spans.  Otherwise we should have read a zero.
		 */
		assert en > 0 || sn > 0 : "Both en and sn zero";
		if (en == 0) {
			for (int prevLeft = 0, sx = 0; sx < sn; ++sx) {
				final int gap = ibs.readGamma();
				spanLeft.add(prevLeft + gap);
				prevLeft += gap;
				final int ss = ibs.readGamma();
				assert 0 <= ss && ss <= Byte.MAX_VALUE;
				spanSize.add((byte)ss);
			}
		}
		else {
			/* else it is a type posting */
			for (int ex = 0; ex < en; ++ex) { // each entity block
				final int sbeg = (ex == 0)? 0 : entOfs.getInt(ex-1);
				final int send = entOfs.getInt(ex);
				for (int sx = sbeg, prevLeft = 0; sx < send; ++sx) { // each posting in entity block
					final int gap = ibs.readGamma();
					spanLeft.add(prevLeft + gap);
					prevLeft += gap;
					final int ss = ibs.readGamma();
					assert 0 <= ss && ss <= Byte.MAX_VALUE;
					spanSize.add((byte)ss);
				}
			}
		}
		ibs.align();
		checkSanity();
	}
	
	public void getWitnesses(TypeBindingQuery tbq, int docId, ReferenceArrayList<AWitness> tbws) {
		assert this.docId == docId;
		tbws.clear();
		for (int ex = 0, px = 0, en = entIds.size(); ex < en; ++ex) {
			final int entLongId = entIds.getInt(ex);
			for (; px < entOfs.getInt(ex); ++px) {
				final int sl = spanLeft.getInt(px);
				final int sr = sl + spanSize.getByte(px);
				final Interval interval = Interval.valueOf(sl, sr);
				final TypeBindingWitness tbw = new TypeBindingWitness(docId, interval, tbq, entLongId);
				tbws.add(tbw);
			}
		}
	}
	
	public void getWitnesses(EntityLiteralQuery elq, int docId, ReferenceArrayList<AWitness> elws) {
		assert this.docId == docId;
		elws.clear();
		// no need to look at entIds or entOfs
		assert entIds.isEmpty() && entOfs.isEmpty();
		for (int px = 0, pn = spanLeft.size(); px < pn; ++px) {
			final int sl = spanLeft.getInt(px);
			final int sr = sl + spanSize.getByte(px);
			final Interval interval = Interval.valueOf(sl, sr);
			final EntityLiteralWitness elw = new EntityLiteralWitness(docId, interval, elq);
			elws.add(elw);
		}
	}
	
	/**
	 * Exact duplicate of {@link Sip2Document#buildFromElr}
	 * @param sdb
	 * @param cursor
	 * @return
	 */
	@Override
	public int buildFromElr(int docId, SipDocumentBuilder<? extends Sip3Document> sdb, int cursor) {
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
			spanLeft.add(left.intValue());
			final int width = right.intValue() - left.intValue();
			if (width > Byte.MAX_VALUE) {
				throw new IllegalArgumentException("Span [" + left + "," + right + "] too large");
			}
			spanSize.add((byte) width);
			++cursor;
		}
		return cursor;
	}
	
	/**
	 * This one is different from {@link Sip2Document#buildFromCelr}: finishes
	 * off entities one by one.
	 * @param docId
	 * @param sdb
	 * @param cursor
	 * @return cursor after zero or one SIP doc has been loaded to sdb
	 */
	@Override
	public int buildFromCelr(int docId, SipDocumentBuilder<? extends Sip3Document> sdb, int cursor) {
		setNull();
		this.docId = docId;
		final MutableInt catId = new MutableInt(), entId = new MutableInt(), left = new MutableInt(), right = new MutableInt();
		while (cursor < sdb.nCelr()) {
			assert spanLeft.size() == spanSize.size();
			sdb.getCelr(cursor, catId, entId, left, right);
			if (entOrCatId == Integer.MIN_VALUE) {
				entOrCatId = catId.intValue();
			}
			if (entOrCatId != catId.intValue()) {
				entOfs.add(spanLeft.size()); // close the last entity sub segment
				checkSanity();
				return cursor; // next block for next cat
			}
			if (entIds.isEmpty()) {
				entIds.add(entId.intValue());
			}
			if (entIds.topInt() != entId.intValue()) {
				entOfs.add(spanLeft.size());
				entIds.add(entId.intValue());
			}
			spanLeft.add(left.intValue());
			final int width = right.intValue() - left.intValue();
			if (width > Byte.MAX_VALUE) {
				throw new IllegalArgumentException("Span [" + left + "," + right + "] too large");
			}
			spanSize.add((byte) width);
			++cursor;
		}
		if (entIds.size() == entOfs.size() + 1) {
			entOfs.add(spanLeft.size()); // close the last entity sub segment
		}
		checkSanity();
		return cursor;
	}
	
	private void checkSanity() {
		assert spanLeft.size() == spanSize.size();
		assert entIds.size() == entOfs.size();
		for (int ex = 0; ex < entOfs.size(); ++ex) {
			assert 0 <= entOfs.getInt(ex);
			assert entOfs.getInt(ex) <= spanLeft.size();
			if (ex > 0) {
				assert entOfs.getInt(ex-1) <= entOfs.getInt(ex);
			}
		}
	}

	@Override
	public void checkSum(RecordDigest rd) throws DigestException {
		rd.appendInt(entOrCatId);
		rd.appendInt(docId);
		assert entIds.size() == entOfs.size();
		for (int ex = 0, en = entIds.size(); ex < en; ++ex) {
			rd.appendInt(entIds.getInt(ex));
			rd.appendInt(entOfs.getInt(ex));
		}
		assert spanLeft.size() == spanSize.size();
		for (int sx = 0, sn = spanLeft.size(); sx < sn; ++sx) {
			rd.appendInt(spanLeft.getInt(sx));
			rd.appendByte(spanSize.getByte(sx));
		}
		rd.endRecord();
	}

	@Override
	public int docId() {
		return docId;
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
	 * For verification of on-disk space.  It would be easier to write to a
	 * temporary {@link OutputBitStream} and count off, but that would not give
	 * us breakups. So we have to replicate {@link #store(OutputBitStream)} logic.
	 * @param nullObs to compute bit costs without hard work
	 * @param nPosts
	 * @param otherBits
	 * @param dictBits
	 * @param ofsBits
	 * @param gapBits
	 * @throws IOException 
	 */
	void estimatePayloadBitsOnDisk(OutputBitStream nullObs, MutableLong nPosts, MutableLong otherBits, MutableLong dictBits, MutableLong ofsBits, MutableLong gapBits) throws IOException {
		assert (otherBits.longValue() + dictBits.longValue() + ofsBits.longValue() + gapBits.longValue()) % Byte.SIZE == 0;
		checkSanity();
		long sumBits = 0;
		otherBits.add(maxEntOrCatBits); // entOrCatId
		sumBits += maxEntOrCatBits;
		otherBits.add(Integer.SIZE); // docId
		sumBits += Integer.SIZE;
		final int en = entIds.size();
		dictBits.add(nullObs.writeGamma(en));  // entIds.size()
		sumBits += nullObs.writeGamma(en);
		if (en > 0) {
			dictBits.add(en * maxEntOrCatBits); // long ent IDs
			sumBits += en * maxEntOrCatBits;
		}
		// no need to write num ents again for ofs array
		assert en == entOfs.size() : "Number of entities " + en + " != " + entOfs.size();
		for (int ex = 0, prevOfs = 0; ex < en; ++ex) {
			final int entOf = entOfs.getInt(ex);
			final int gap = entOf-prevOfs;
			assert gap >= 0;
			ofsBits.add(nullObs.writeGamma(gap));
			sumBits += nullObs.writeGamma(gap);
			prevOfs = entOf;
		}
		/*
		 * If en == 0, this is an entity posting, so we write out the number of
		 * spans.  Otherwise we write a zero to be consistent while loading.
		 */
		gapBits.add(nullObs.writeGamma(en == 0? spanLeft.size() : 0));
		sumBits += nullObs.writeGamma(en == 0? spanLeft.size() : 0);
		if (en == 0) {
			for (int prevLeft = 0, sx = 0, sn = spanLeft.size(); sx < sn; ++sx) {
				final int sl = spanLeft.getInt(sx);
				assert sl >= prevLeft;
				final int gap = sl - prevLeft;
				gapMon.add(gap);
				gapBits.add(nullObs.writeGamma(gap));
				sumBits += nullObs.writeGamma(gap);
				final byte ss = spanSize.getByte(sx);
				gapBits.add(nullObs.writeGamma(ss));
				sumBits += nullObs.writeGamma(ss);
				nPosts.add(1);
				prevLeft = sl;
			}
		}
		else {
			/* otherwise this is a type posting */
			for (int ex = 0; ex < en; ++ex) { // each entity block
				final int sbeg = (ex == 0)? 0 : entOfs.getInt(ex-1);
				final int send = entOfs.getInt(ex);
				for (int sx = sbeg, prevLeft = 0; sx < send; ++sx) { // each posting in entity block
					final int sl = spanLeft.getInt(sx);
					assert sl >= prevLeft;
					final int gap = sl - prevLeft;
					gapMon.add(gap);
					gapBits.add(nullObs.writeGamma(gap));
					sumBits += nullObs.writeGamma(gap);
					final byte ss = spanSize.getByte(sx);
					gapBits.add(nullObs.writeGamma(ss));
					sumBits += nullObs.writeGamma(ss);
					nPosts.add(1);
					prevLeft = sl;
				}
			}
		}
		final long padBits = ((sumBits + Byte.SIZE - 1) / Byte.SIZE) * Byte.SIZE - sumBits;
		otherBits.add(padBits);
		assert (otherBits.longValue() + dictBits.longValue() + ofsBits.longValue() + gapBits.longValue()) % Byte.SIZE == 0;
	}

	/**
	 * For testing.
	 * @author sasik
	 */
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		// Ent or cat id.
		sb.append("t=" + entOrCatId + " d=" + docId);
		sb.append(" ne:" + entIds.size() + " ");
		assert spanLeft.size() == spanSize.size() : "Type=" + entOrCatId + " Doc=" + docId + " spanLeftSize=" + spanLeft.size() + " spanSize size=" + spanSize.size();
		sb.append(" ns:" + spanSize.size());
		// Write ent block by ent block. 
		for(int i=0, prev=0, n=entIds.size(); i<n; ++i) {
			final int ent = entIds.get(i);
			final int end = entOfs.get(i);
			for(int j=prev; j<end; ++j) {
				sb.append(" sn=" + j + " l=" + spanLeft.get(j) + " s=" + spanSize.get(j) + " e=" + ent);
			}
			prev = end;
		}
		return sb.toString();
	}
}
