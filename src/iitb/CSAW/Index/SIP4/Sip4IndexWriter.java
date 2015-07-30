package iitb.CSAW.Index.SIP4;

import iitb.CSAW.Index.AnnotationLeaf;
import iitb.CSAW.Index.SipDocumentBuilder;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.IOException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

public class Sip4IndexWriter extends SipDocumentBuilder<Sip4Document> {
	final Sip4Document sd = new Sip4Document();
	
	public Sip4IndexWriter(Config config, AtomicInteger batchCounter, IntOpenHashSet registeredCats) throws Exception {
		super(Sip4Document.class, config, batchCounter, registeredCats);
	}
	
	public void close() throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		flush(true, sd);
	}
	
	public void indexOneDocument(int docId, ReferenceArrayList<AnnotationLeaf> annots) throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		reuse();
		digestDocument(docId, annots);
		checkSizes();
		transposeElrToEntLeftOrder();
		transposeCelrToCatLeftOrder();
		writeEntPostings(docId, sd);
		writeTypePostings(docId, sd);
		flush(false, sd);
	}
}
