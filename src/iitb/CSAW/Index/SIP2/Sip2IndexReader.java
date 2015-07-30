package iitb.CSAW.Index.SIP2;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Index.BaseIndexMerger;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.util.Properties;
import it.unimi.dsi.util.SemiExternalGammaList;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

public class Sip2IndexReader {
	final Logger logger = Logger.getLogger(getClass());
	final Config config;
	final AStripeManager stripeManager;
	final File sipIndexDir;

	/** To seek in compressed posting files. */
	final SemiExternalGammaList entSeekSegl, typeSeekSegl;
	/** To determine how many doc records can be scanned. */
	final SemiExternalGammaList entLocalDfSegl, typeLocalDfSegl;
	/** For per-entity and per-type statistics. */
	final SemiExternalGammaList entGlobalDfSegl, typeGlobalDfSegl, entGlobalCfSegl, typeGlobalCfSegl;
	/** For global aggregates */
	final Properties entProps, typeProps;
	
	public Sip2IndexReader(Config config) throws IOException, ConfigurationException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, URISyntaxException {
		this.config = config;
		stripeManager = AStripeManager.construct(config);
		final URI sipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		assert sipIndexDiskUri.getHost().equals(stripeManager.myHostName());
		sipIndexDir = new File(sipIndexDiskUri.getPath());
		logger.info("Reading " + sipIndexDir);

		entSeekSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.ent + iitb.CSAW.Index.PropertyKeys.sipCompactSeekExtension))));
		logger.info("entSeek " + entSeekSegl.size() + " " + countNulls(entSeekSegl));
		entLocalDfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.ent + iitb.CSAW.Index.PropertyKeys.sipCompactLocalDfExtension))));
		logger.info("entLocalDf " + entLocalDfSegl.size() + " " + countNulls(entLocalDfSegl));
		entGlobalDfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.ent + iitb.CSAW.Index.PropertyKeys.sipCompactGlobalDfExtension))));
		logger.info("entGlobalDf " + entGlobalDfSegl.size() + " " + countNulls(entGlobalDfSegl));
		entGlobalCfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.ent + iitb.CSAW.Index.PropertyKeys.sipCompactGlobalCfExtension))));
		logger.info("entGlobalCf " + entGlobalCfSegl.size() + " " + countNulls(entGlobalCfSegl));
		
		typeSeekSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.type + iitb.CSAW.Index.PropertyKeys.sipCompactSeekExtension))));
		logger.info("typeSeek " + typeSeekSegl.size() + " " + countNulls(typeSeekSegl));
		typeLocalDfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.type + iitb.CSAW.Index.PropertyKeys.sipCompactLocalDfExtension))));
		logger.info("typeLocalDf " + typeLocalDfSegl.size() + " " + countNulls(typeSeekSegl));
		typeGlobalDfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.type + iitb.CSAW.Index.PropertyKeys.sipCompactGlobalDfExtension))));
		logger.info("typeGlobalDf " + typeGlobalDfSegl.size() + " " + countNulls(typeGlobalDfSegl));
		typeGlobalCfSegl = new SemiExternalGammaList(new InputBitStream(BinIO.loadBytes(new File(sipIndexDir, Field.type + iitb.CSAW.Index.PropertyKeys.sipCompactGlobalCfExtension))));
		logger.info("typeGlobalCf " + typeGlobalCfSegl.size() + " " + countNulls(typeGlobalCfSegl));
		
		entProps = new Properties(new File(sipIndexDir, Field.ent + DiskBasedIndex.PROPERTIES_EXTENSION));
		typeProps = new Properties(new File(sipIndexDir, Field.type + DiskBasedIndex.PROPERTIES_EXTENSION));
	}
	
	long globalXf(SemiExternalGammaList segl, Field field, boolean strict, int entOrTypeId) {
		final long tmpAns = segl.getLong(entOrTypeId) - BaseIndexMerger.GAMMA_OFFSET;
		if (tmpAns == -1) {
			if (strict) {
				throw new IllegalArgumentException("Field " + field + " cannot map ID " + entOrTypeId);
			}
			else {
				return 0;
			}
		}
		return tmpAns;
	}
	
	public long entGlobalCf(int entId, boolean strict) {
		return globalXf(entGlobalCfSegl, Field.ent, strict, entId);
	}
	
	public long entGlobalDf(int entId, boolean strict) {
		return globalXf(entGlobalDfSegl, Field.ent, strict, entId);
	}
	
	public long typeGlobalCf(int catId, boolean strict) {
		return globalXf(typeGlobalCfSegl, Field.type, strict, catId);
	}
	
	public long typeGlobalDf(int catId, boolean strict) {
		return globalXf(typeGlobalDfSegl, Field.type, strict, catId);
	}
	
	public long entGlobalCf() {
		return entProps.getLong(iitb.CSAW.Index.PropertyKeys.globalNumOccurrences);
	}
	
	public long entGlobalDf() {
		return entProps.getLong(iitb.CSAW.Index.PropertyKeys.globalMaxDocument1);
	}

	public long typeGlobalCf() {
		return typeProps.getLong(iitb.CSAW.Index.PropertyKeys.globalNumOccurrences);
	}

	public long typeGlobalDf() {
		return typeProps.getLong(iitb.CSAW.Index.PropertyKeys.globalMaxDocument1);
	}
	
	public SipSpanIterator getEntIterator(int entId) throws IOException {
		return new SipSpanIterator(Field.ent, entId);
	}
	
	public SipSpanIterator getTypeIterator(int catId) throws IOException {
		return new SipSpanIterator(Field.type, catId);
	}

	public class SipSpanIterator {
		final Field field;
		final InputBitStream postIbs;
		final int key, nDocs;
		int dx = 0;
		final Sip2Document ssd = new Sip2Document();

		SipSpanIterator(Field field, int key) throws IOException {
			ssd.init(-1, Integer.MIN_VALUE);
			this.field = field;
			final File postFile = new File(sipIndexDir, field.toString() + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
			postIbs = new InputBitStream(postFile);
			this.key = key;
			final long rawSeek = choose(field, entSeekSegl, typeSeekSegl).getLong(key);
			if (rawSeek == 0) {
				nDocs = dx = 0;
				ssd.docId = Integer.MAX_VALUE;
//				logger.warn(key + " not mapped in " + field + " index");
				// TODO maintain stat of missing key
				return;
			}
			postIbs.position(rawSeek - BaseIndexMerger.GAMMA_OFFSET);
			final long nDocsLocalRaw = choose(field, entLocalDfSegl, typeLocalDfSegl).getLong(key);
			if (nDocsLocalRaw == 0) {
				throw new IllegalArgumentException(key + " is mapped in seek but not in nDocs");
			}
			nDocs = (int) nDocsLocalRaw - Sip2IndexMerger.GAMMA_OFFSET;
			dx = 0;
		}
		
		public int document() {
			return ssd.docId;
		}
		
		public void skipTo(int target) throws IOException {
			while (dx < nDocs) {
				if (ssd.docId >= target) {
					return;
				}
				++dx;
				final long restBits = ssd.loadHead(postIbs);
				if (ssd.docId >= target) {
					ssd.loadRest(postIbs);
					return;
				}
				else {
					ssd.skipRest(postIbs, restBits);
				}
			}
			ssd.init(-1, Integer.MAX_VALUE);
		}
		
		/** For speed test */
		public long skipAll() throws IOException {
			long numSkips = 0;
			while (dx < nDocs) {
				if (ssd.docId == Integer.MAX_VALUE) { return numSkips; }
				++dx;
				final long restBits = ssd.loadHead(postIbs);
				ssd.skipRest(postIbs, restBits);
				++numSkips;
			}
			ssd.init(-1, Integer.MAX_VALUE);
			return numSkips;
		}
		
		public int nextDocument() throws IOException {
			if (dx >= nDocs) {
				ssd.init(-1, Integer.MAX_VALUE);
				return Integer.MAX_VALUE;
			}
			++dx;
			ssd.load(postIbs);
			if (ssd.entOrCatId != key) {
				throw new IllegalStateException("Posting list corrupted for key=" + key + " doc=" + ssd.docId);
			}
			return ssd.docId;
		}
		
		public void getPostings(Sip2Document outSsd) {
			outSsd.replace(ssd);
		}
		
		public void dispose() throws IOException {
			postIbs.close();
		}
	}
	
	<T> T choose(Field field, T entThang, T typeThang) {
		switch (field) {
		case ent:
			return entThang;
		case type:
			return typeThang;
		}
		throw new IllegalArgumentException("Field=" + field + " not allowed.");
	}
	
	int countNulls(SemiExternalGammaList segl) {
		int nZeros = 0;
		for (int ix = 0, nx = segl.size(); ix < nx; ++ix) {
			if (segl.getLong(ix) == 0) {
				++nZeros;
			}
		}
		return nZeros;
	}
}
