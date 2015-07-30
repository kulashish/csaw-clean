package iitb.CSAW.Index.SIP1;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.io.InputBitStream;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;

public class Sip1IndexReader {
	final Config config;
	final AStripeManager stripeManager;
	final Int2LongOpenHashMap catToSeek;
	final Int2IntOpenHashMap catToNDoc;
	final File smallPostingsFile;

	public Sip1IndexReader(Config conf, String typeName) throws IOException, ClassNotFoundException, IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, URISyntaxException {
		this.config = conf;
		stripeManager = AStripeManager.construct(config);
		final URI sipIndexDiskUri = stripeManager.sipIndexDiskDir(stripeManager.myDiskStripe());
		assert sipIndexDiskUri.getHost().equals(stripeManager.myHostName());
		final File sipIndexDir = new File(sipIndexDiskUri.getPath());
		final File smallNdocsFile = new File(sipIndexDir, typeName + iitb.CSAW.Index.PropertyKeys.sipCompactNumDocExtension);
		final File smallOffsetsFile = new File(sipIndexDir, typeName + iitb.CSAW.Index.PropertyKeys.sipCompactOffsetExtension);
		this.smallPostingsFile = new File(sipIndexDir, typeName + iitb.CSAW.Index.PropertyKeys.sipCompactPostingExtension);
		catToNDoc = (Int2IntOpenHashMap) BinIO.loadObject(smallNdocsFile);
		catToSeek = (Int2LongOpenHashMap) BinIO.loadObject(smallOffsetsFile);
	}
	
	public SipIterator getIterator(int catId) throws IOException {
		return new SipIterator(catId);
	}
	
	public class SipIterator {
		final InputBitStream typePostIbs;
		final int catId, nDocs;
		int dx, curDocId;
		final Sip1Document idp = new Sip1Document();
		
		SipIterator(int catId) throws IOException {
			this.typePostIbs = new InputBitStream(smallPostingsFile);
			this.catId = catId;
			if (!catToSeek.containsKey(catId)) {
				throw new IllegalArgumentException(catId + " is not mapped");
			}
			final long catSeek = catToSeek.get(catId);
			typePostIbs.position(catSeek);
			this.nDocs = catToNDoc.get(catId);
			this.dx = 0;
			this.curDocId = Integer.MIN_VALUE;
		}
		
		public int document() {
			return curDocId;
		}

		public int nextDocument() throws IOException {
			if (dx > nDocs - 1) {
				idp.clear();
				curDocId = Integer.MAX_VALUE;
				return -1;
			}
			++dx;
			idp.loadCompressed(typePostIbs, catId);
			curDocId = idp.docId();
			return curDocId;
		}

		/**
		 * Writes something useful in outIdp only if the immediately preceding
		 * call to {@link #nextDocument()} did not return -1 or throw any 
		 * exception.
		 * @param outIdp
		 */
		public void getPostings(Sip1Document outIdp) {
			outIdp.replace(idp);
		}
		
		public void dispose() throws IOException {
			typePostIbs.close();
		}
	}
}
