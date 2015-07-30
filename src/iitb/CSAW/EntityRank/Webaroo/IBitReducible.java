package iitb.CSAW.EntityRank.Webaroo;

import iitb.CSAW.Utils.Sort.ABitReducer;
import iitb.CSAW.Utils.Sort.IBitRecord;

public interface IBitReducible<T extends IBitRecord<T>> extends IBitRecord<T> {
	public ABitReducer<T, T> getReducer();
}
