package iitb.CSAW.Utils.Sort;

public abstract class ABitReducer<Tin extends IBitRecord<Tin>, Tout extends IBitRecord<Tout>> {
	/** Re-initializes the state of a Tout object. */
	public abstract void reset();
	
	/** Accumulates the given Tin object into the internal Tout object. Should
	 * correctly handle the case of the internal Tout object being null. */
	public abstract void accumulate(Tin inrec);
	
	/** Compare the key of inrec against the key of the internal Tout object.
	 * Return internal key "minus" inrec.key.  Return 1 if internal Tout
	 * object is still "empty" (key = minus infinity). */
	public abstract int compareKeys(Tin inrec);
	
	/** Return the currently aggregated Tout object. 
	 * Test for {@link IBitRecord#isNull()} before use! */
	public abstract void getResult(Tout outrec);
}
