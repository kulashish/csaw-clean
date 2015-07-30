package iitb.CSAW.Index;

import java.security.DigestException;

import iitb.CSAW.Utils.RecordDigest;
import iitb.CSAW.Utils.Sort.IBitRecord;

/**
 * Attempts to unify the SIP variants in one interface so that index merging
 * and search can be shared.
 * @author soumen
 * @param <T>
 */
public interface ISipDocument<T> extends IBitRecord<T> {
	public int buildFromElr(int docId, SipDocumentBuilder<? extends T> sdb, int cursor);
	public int buildFromCelr(int docId, SipDocumentBuilder<? extends T> sdb, int cursor);
	public int docId();
	public int entOrCatId();
	public int nPosts();
	public void checkSum(RecordDigest rd) throws DigestException;
}
