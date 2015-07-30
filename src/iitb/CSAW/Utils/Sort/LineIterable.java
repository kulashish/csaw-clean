package iitb.CSAW.Utils.Sort;

import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

/**
 * Reads a text file and presents an iterator over each line.
 * @author soumen
 */
public class LineIterable implements Iterable<String> {
	public static void main(String[] args) throws Exception {
		LineIterable fsc = new LineIterable(new File(args[0]));
		for (String line : fsc) {
			System.out.println(line);
		}
	}
	
	final File inf;
	
	public LineIterable(File inf) {
		this.inf = inf;
	}
	
	@Override
	public Iterator<String> iterator() {
		return new FileStringIterator();
	}
	
	class FileStringIterator implements Iterator<String> {
		FastBufferedReader fr = null;
		final MutableString line = new MutableString();
		MutableString status = null;
		
		FileStringIterator() {
			try {
				fr = new FastBufferedReader(new FileReader(inf));
				status = fr.readLine(line);
			}
			catch (Exception anyx) {
				throw new IllegalArgumentException(anyx);
			}
		}

		@Override
		public boolean hasNext() {
			return status != null;
		}

		@Override
		public String next() {
			try {
				final String result = status == null? null : status.toString();
				if (status != null) {  
					status = fr.readLine(line);
					if (status == null) {
						fr.close();
					}
				}
				return result;
			} catch(IOException e) { 
				throw new IllegalArgumentException(e); 
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
