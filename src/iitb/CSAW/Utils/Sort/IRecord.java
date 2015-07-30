package iitb.CSAW.Utils.Sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

/**
 * A mutable record that can replace its value with that of another, and
 * read (write) itself from (to) a {@link DataInput} ({@link DataOutput}).
 * Useful for external sorting, compressed file storage, etc.
 * @author soumen
 */
public interface IRecord {
	/**
	 * Must throw {@link EOFException} if no more records.
	 * No other signal to caller. 
	 * @param dii
	 * @throws IOException
	 */
	public void load(DataInput dii) throws IOException;
	public void store(DataOutput doi) throws IOException;
	/**
	 * Will usually coerce input to its own type and then copy members.
	 * @param src source object.
	 */
	public <IR extends IRecord> void replace(IR src);
}
