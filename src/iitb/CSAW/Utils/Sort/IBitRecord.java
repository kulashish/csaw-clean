package iitb.CSAW.Utils.Sort;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.lang.NotImplementedException;

import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;

/**
 * A mutable record that can replace its value with that of another, and
 * read (write) itself from (to) a {@link InputBitStream}
 * ({@link OutputBitStream}).  Useful for external sorting, compressed
 * file storage, etc.
 * 
 * @author soumen
 */
public interface IBitRecord<T> {
	public void store(OutputBitStream obs) throws IOException;
	public void load(InputBitStream ibs) throws IOException;
	public void replace(T ibr);
	public void setNull();
	public boolean isNull();
	/** Can throw {@link NotImplementedException} or return null if not needed. */
	public Comparator<T> getComparator();
}
