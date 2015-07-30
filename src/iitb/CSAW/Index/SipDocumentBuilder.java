package iitb.CSAW.Index;

import gnu.trove.TIntHashSet;
import gnu.trove.TIntProcedure;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Index.SIP2.Sip2Document;
import iitb.CSAW.Index.SIP2.Sip2IndexWriter;
import iitb.CSAW.Index.SIP3.Sip3Document;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.RecordDigest;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.mutable.MutableFloat;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;

import cern.colt.Sorting;
import cern.colt.function.IntComparator;
import cern.colt.function.LongComparator;

/**
 * <p>The first step in turning a set of {@link Annotation}s into a SIP document
 * is to reorganize into records of the form (entity, left, right) sorted by
 * (entity, left) for the entity index, and (type, entity, left, right) sorted
 * by either (type, left) for {@link Sip2Document} or (type, entity) for
 * {@link Sip3Document}.  This class implements the required reorganization.
 * Much of the first version of this class was moved from {@link Sip2IndexWriter}.
 * Initially we will just dump different methods for {@link Sip2Document} and
 * {@link Sip3Document} as needed, and perhaps later try to refactor.</p>
 *
 * <p><b>Not</b> thread-safe. One instance of this class should be used by only
 * one thread.</p>
 * 
 * @author soumen
 * @since 2011/05/10
 */
public class SipDocumentBuilder<SD extends ISipDocument<SD>> {
	static final int entRunBytes = 8000000, typeRunBytes = 80000000;
	static final double runMaxFill = 0.9;
	
	protected final Logger logger = Logger.getLogger(getClass()); 
	final Config conf;
	final ACatalog catalog;
	final IntOpenHashSet registeredCats;
	final AStripeManager stripeManager;
	final AtomicInteger batchCounter;

	final byte[] entRunBuffer = new byte[entRunBytes];
	protected final OutputBitStream entRunObs = new OutputBitStream(entRunBuffer);
	final byte[] typeRunBuffer = new byte[typeRunBytes];
	protected final OutputBitStream typeRunObs = new OutputBitStream(typeRunBuffer);
	
	private final IntArrayList elrEnt = new IntArrayList();
	private final IntArrayList elrLeft = new IntArrayList();
	private final IntArrayList elrRight = new IntArrayList();
	private final IntArrayList elrLeaf = new IntArrayList();
	private final IntArrayList elrRank = new IntArrayList();
	private final FloatArrayList elrScore = new FloatArrayList();
	private final IntArrayList elrSorter = new IntArrayList();
	private final LongArrayList elrSeeker = new LongArrayList();
	
	private final IntArrayList celrCat = new IntArrayList();
	private final IntArrayList celrEnt = new IntArrayList();
	private final IntArrayList celrLeft = new IntArrayList();
	private final IntArrayList celrRight = new IntArrayList();
	private final IntArrayList celrLeaf = new IntArrayList();
	private final IntArrayList celrRank = new IntArrayList();
	private final FloatArrayList celrScore = new FloatArrayList();
	private final IntArrayList celrSorter = new IntArrayList();
	private final LongArrayList celrSeeker = new LongArrayList();
	
	final Class<SD> type;
	
	public SipDocumentBuilder(Class<SD> type, Config conf, AtomicInteger batchCounter, IntOpenHashSet registeredCats) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, InstantiationException, IOException {
		this.type = type;
		this.conf = conf;
		this.catalog = ACatalog.construct(conf);
		this.registeredCats = registeredCats;
		stripeManager = AStripeManager.construct(conf);
		this.batchCounter = batchCounter;
		entRunObs.writtenBits(0);
		entRunObs.position(0);
		typeRunObs.writtenBits(0);
		typeRunObs.position(0);
	}
	
	/**
	 * <b>Note:</b> Entity strings that cannot be mapped by {@link #catalog}
	 * will be (almost) silently discarded.
	 * @param docId
	 * @param lannots
	 */
	public void digestDocument(int docId, ReferenceArrayList<AnnotationLeaf> lannots) {
		Collections.sort(lannots);
		final TIntHashSet catIds = new TIntHashSet(); 
		for (final AnnotationLeaf anno : lannots) {
			final int entId = catalog.entNameToEntID(anno.entName);
			if (entId < 0) {
				logger.trace("Catalog cannot map " + anno.entName);
				continue;
			}
			elrEnt.add(entId);
			elrLeft.add(anno.interval.left);
			elrRight.add(anno.interval.right);
			elrLeaf.add(anno.leaf);
			elrRank.add(anno.rank);
			elrScore.add(anno.score);
			elrSorter.add(elrSorter.size());
			catIds.clear();
			catalog.catsReachableFromEnt(entId, catIds);
			catIds.forEach(new TIntProcedure() {
				@Override
				public boolean execute(int catId) {
					if (registeredCats == null || registeredCats.contains(catId)) {
						celrCat.add(catId);
						celrEnt.add(entId);
						celrLeft.add(anno.interval.left);
						celrRight.add(anno.interval.right);
						celrLeaf.add(anno.leaf);
						celrRank.add(anno.rank);
						celrScore.add(anno.score);
						celrSorter.add(celrSorter.size());
					}
					return true;
				}
			});
		} // for-anno
	}

	/**
	 * For entity postings, same in all SIP variants. Sort key is ent, left.
	 */
	protected void transposeElrToEntLeftOrder() {
		Sorting.quickSort(elrSorter.elements(), 0, elrSorter.size(), new IntComparator() {
			@Override
			public int compare(int ix1, int ix2) {
				final int eDiff = elrEnt.getInt(ix1) - elrEnt.getInt(ix2);
				if (eDiff != 0) {
					return eDiff;
				}
				return elrLeft.getInt(ix1) - elrLeft.getInt(ix2);
			}
		});
	}
	
	/**
	 * For type/cat postings, this one for {@link Sip2Document}.
	 * Sort key is cat, left.
	 */
	protected void transposeCelrToCatLeftOrder() {
		Sorting.quickSort(celrSorter.elements(), 0, celrSorter.size(), new IntComparator() {
			@Override
			public int compare(int ix1, int ix2) {
				final int cDiff = celrCat.getInt(ix1) - celrCat.getInt(ix2);
				if (cDiff != 0) {
					return cDiff;
				}
				return celrLeft.getInt(ix1) - celrLeft.getInt(ix2);
			}
		});
	}
	
	/**
	 * For type/cat postings, this one for {@link Sip3Document}.
	 * Sort key is cat, ent, left.
	 */
	protected void transposeCelrToCatEntLeftOrder() {
		Sorting.quickSort(celrSorter.elements(), 0, celrSorter.size(), new IntComparator() {
			@Override
			public int compare(int ix1, int ix2) {
				final int cDiff = celrCat.getInt(ix1) - celrCat.getInt(ix2);
				if (cDiff != 0) {
					return cDiff;
				}
				final int eDiff = celrEnt.getInt(ix1) - celrEnt.getInt(ix2);
				if (eDiff != 0) {
					return eDiff;
				}
				return celrLeft.getInt(ix1) - celrLeft.getInt(ix2);
			}
		});
	}
	
	public void reuse() {
		elrEnt.clear();
		elrLeft.clear();
		elrRight.clear();
		elrLeaf.clear();
		elrRank.clear();
		elrScore.clear();
		elrSorter.clear();
		elrSeeker.clear();
		
		celrCat.clear();
		celrEnt.clear();
		celrLeft.clear();
		celrRight.clear();
		celrLeaf.clear();
		celrScore.clear();
		celrRank.clear();
		celrSorter.clear();
		celrSeeker.clear();
	}
	
	protected void checkSizes() {
		final int elrSize = elrEnt.size();
		assert elrSize == elrLeft.size() && elrSize == elrRight.size() && elrSize == elrLeaf.size() && elrSize == elrRank.size() && elrSize == elrScore.size() && elrSize == elrSorter.size();
		final int celrSize = celrCat.size();
		assert celrSize == celrEnt.size() && celrSize == celrLeft.size() && celrSize == celrRight.size() && celrSize == celrLeaf.size() && celrSize == celrRank.size() && celrSize == celrScore.size() && celrSize == celrSorter.size();
	}

	public int nElr() { return elrEnt.size(); }
	
	public void getElr(int elrX, MutableInt entId, MutableInt left, MutableInt right) {
		final int elrPos = elrSorter.getInt(elrX);
		entId.setValue(elrEnt.getInt(elrPos));
		left.setValue(elrLeft.getInt(elrPos));
		right.setValue(elrRight.getInt(elrPos));
	}
	
	public void getElr(int elrX, MutableInt ent, MutableInt left, MutableInt right, MutableInt leaf, MutableInt rank, MutableFloat score) {
		getElr(elrX, ent, left, right);
		final int elrPos = elrSorter.getInt(elrX);
		leaf.setValue(elrLeaf.getInt(elrPos));
		rank.setValue(elrRank.getInt(elrPos));
		score.setValue(elrScore.getFloat(elrPos));
	}
	
	public int nCelr() { return celrCat.size(); }
	
	public void getCelr(int celrX, MutableInt catId, MutableInt entId, MutableInt left, MutableInt right) {
		final int celrPos = celrSorter.getInt(celrX);
		catId.setValue(celrCat.getInt(celrPos));
		entId.setValue(celrEnt.getInt(celrPos));
		left.setValue(celrLeft.getInt(celrPos));
		right.setValue(celrRight.getInt(celrPos));
	}
	
	public void getCelr(int celrX, MutableInt cat, MutableInt ent, MutableInt left, MutableInt right, MutableInt leaf, MutableInt rank, MutableFloat score) {
		getCelr(celrX, cat, ent, left, right);
		final int celrPos = celrSorter.getInt(celrX);
		leaf.setValue(celrLeaf.getInt(celrPos));
		rank.setValue(celrRank.getInt(celrPos));
		score.setValue(celrScore.getFloat(celrPos));
	}
	
	public void writeEntPostings(final int docId, ISipDocument<SD> s2d) throws IOException {
		for (int cursor = 0; cursor < nElr(); ) {
			cursor = s2d.buildFromElr(docId, this, cursor);
			if (!s2d.isNull()) {
				s2d.store(entRunObs);
			}
		}
	}
	
	public void writeTypePostings(final int docId, ISipDocument<SD> s2d) throws IOException {
		for (int cursor = 0; cursor < nCelr(); ) {
			cursor = s2d.buildFromCelr(docId, this, cursor);
			if (!s2d.isNull()) {
				s2d.store(typeRunObs);
			}
		}
	}
	
	public void sortRun(byte[] buf, long bits, LongArrayList seeker) throws IOException, InstantiationException, IllegalAccessException {
		final SD ssd1 = type.newInstance();
		final SD ssd2 = type.newInstance();
		final Comparator<SD> cmp = ssd1.getComparator();
		
		seeker.clear();
		final InputBitStream ibs = new InputBitStream(buf);
		try {
			for (; ibs.readBits() < bits;) {
				final long beginPos = ibs.readBits();
				ssd1.load(ibs);
				seeker.add(beginPos);
			}
		}
		catch (EOFException eofx) {
			logger.debug(seeker.size() + " records");
		}
		Sorting.quickSort(seeker.elements(), 0, seeker.size(), new LongComparator() {
			@Override
			public int compare(long o1, long o2) {
				try {
					ibs.position(o1);
					ssd1.load(ibs);
					ibs.position(o2);
					ssd2.load(ibs);
					return cmp.compare(ssd1, ssd2);
				}
				catch (IOException iox) {
					logger.error("Error in sorting seeker", iox);
					return 0;
				}
			}
		});
		ibs.close();
	}

	public void flush(boolean doForce, SD ssd) throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		entRunObs.flush();
		assert entRunObs.writtenBits() < Byte.SIZE * entRunBuffer.length;
		if (doForce || entRunObs.writtenBits() / Byte.SIZE > runMaxFill * entRunBuffer.length) {
			assert entRunObs.writtenBits() % Byte.SIZE == 0;
			// sort and do the flushing to disk
			sortRun(entRunBuffer, entRunObs.writtenBits(), elrSeeker);
			final int batch = batchCounter.getAndIncrement();
			final File entRunFile = new File(stripeManager.mySipIndexRunDir(), Field.ent.toString() + "_" + batch + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
			logger.info("Flushing " + entRunObs.writtenBits() / Byte.SIZE + " bytes to " + entRunFile);
			writeRun(entRunFile, entRunBuffer, elrSeeker, ssd);
			// clean out the buffer
			Arrays.fill(entRunBuffer, (byte) 0);
			entRunObs.writtenBits(0);
			entRunObs.position(0);
		}
		
		typeRunObs.flush();
		assert typeRunObs.writtenBits() < Byte.SIZE * typeRunBuffer.length;
		if (doForce || typeRunObs.writtenBits() / Byte.SIZE > runMaxFill * typeRunBuffer.length) {
			assert typeRunObs.writtenBits() % Byte.SIZE == 0;
			// sort and do the flushing to disk
			sortRun(typeRunBuffer, typeRunObs.writtenBits(), celrSeeker);
			final int batch = batchCounter.getAndIncrement();
			final File typeRunFile = new File(stripeManager.mySipIndexRunDir(), Field.type.toString() + "_" + batch + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
			logger.info("Flushing " + typeRunObs.writtenBits() / Byte.SIZE + " bytes to " + typeRunFile);
			writeRun(typeRunFile, typeRunBuffer, celrSeeker, ssd);
			// clean out the buffer
			Arrays.fill(typeRunBuffer, (byte) 0);
			typeRunObs.writtenBits(0);
			typeRunObs.position(0);
		}
	}
	
	void writeRun(File runFile, byte[] runBuffer, LongArrayList seeker, ISipDocument<SD> ssd) throws IOException, NoSuchAlgorithmException, DigestException {
		RecordDigest pd = new RecordDigest();
		final InputBitStream runIbs = new InputBitStream(runBuffer);
		final OutputBitStream runObs = new OutputBitStream(runFile);
		int prevEntOrCatId = -1, prevDocId = -1;
		for (long seek : seeker) {
			runIbs.position(seek);
			ssd.load(runIbs);
			assert ssd.entOrCatId() > prevEntOrCatId || ssd.docId() > prevDocId;
			ssd.store(runObs);
			ssd.checkSum(pd);
			prevEntOrCatId = ssd.entOrCatId();
			prevDocId = ssd.docId();
		}
		runObs.close();
		runIbs.close();
		BinIO.storeBytes(pd.getDigest(), runToShaFile(runFile));
	}
	
	public static File runToShaFile(File runFile) {
		final File dir = runFile.getParentFile();
		final String name = runFile.getName();
		final int extPos = name.indexOf(iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
		if (extPos < 0) {
			throw new IllegalArgumentException("Cannot find SHA file corresponding to " + runFile);
		}
		final String target = name.substring(0, extPos) + iitb.CSAW.Index.PropertyKeys.sipCompactShaExtension;
		return new File(dir, target);
	}
}
