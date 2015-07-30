package iitb.CSAW.Index.SIP2;

import iitb.CSAW.Index.AnnotationLeaf;
import iitb.CSAW.Index.SipDocumentBuilder;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

public class Sip2IndexWriter extends SipDocumentBuilder<Sip2Document> {
	final Sip2Document s2d = new Sip2Document();
	
	public Sip2IndexWriter(Config config, AtomicInteger batchCounter, IntOpenHashSet registeredCats) throws Exception {
		super(Sip2Document.class, config, batchCounter, registeredCats);
	}
	
	public void close() throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		flush(true, s2d);
	}
	
	public void indexOneDocument(int docId, ReferenceArrayList<AnnotationLeaf> annots) throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		reuse();
		digestDocument(docId, annots);
		checkSizes();
		transposeElrToEntLeftOrder();
		transposeCelrToCatLeftOrder();
		writeEntPostings(docId, s2d);
		writeTypePostings(docId, s2d);
		flush(false, s2d);
	}
}
