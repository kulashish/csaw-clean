package iitb.CSAW.Index.SIP3;

import iitb.CSAW.Index.AnnotationLeaf;
import iitb.CSAW.Index.SipDocumentBuilder;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

public class Sip3IndexWriter extends SipDocumentBuilder<Sip3Document> {
	final Sip3Document s3d = new Sip3Document();

	public Sip3IndexWriter(Config conf, AtomicInteger batchCounter, IntOpenHashSet registeredCats) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		super(Sip3Document.class, conf, batchCounter, registeredCats);
	}

	public void indexOneDocument(int docId, ReferenceArrayList<AnnotationLeaf> annots) throws IOException, NoSuchAlgorithmException, DigestException, InstantiationException, IllegalAccessException {
		reuse();
		digestDocument(docId, annots);
		checkSizes();
		transposeElrToEntLeftOrder();
		transposeCelrToCatEntLeftOrder();
		writeEntPostings(docId, s3d);
		writeTypePostings(docId, s3d);
		flush(false, s3d);
	}
	
	public void close() throws NoSuchAlgorithmException, DigestException, IOException, InstantiationException, IllegalAccessException {
		flush(true, s3d);
	}
}
