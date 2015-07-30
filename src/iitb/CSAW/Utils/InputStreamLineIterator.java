package iitb.CSAW.Utils;

import it.unimi.dsi.io.FileLinesCollection;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

/**
 * Alternative to {@link FileLinesCollection} which cannot be constructed
 * out of an {@link InputStream}.
 * 
 * @author soumen
 */
public class InputStreamLineIterator implements Iterator<String> {
	private final LineNumberReader lnr;
	private String buf;
	
	public InputStreamLineIterator(InputStream is) {
		lnr = new LineNumberReader(new InputStreamReader(is));
		prepare();
	}
	
	private void prepare() {
		try {
			buf = lnr.readLine();
		}
		catch (IOException iox) {
			buf = null;
		}
	}

	@Override
	public boolean hasNext() {
		return buf != null;
	}

	@Override
	public String next() {
		final String ans = buf;
		prepare();
		return ans;
	}

	@Override
	public void remove() {
		throw new NotImplementedException();
	}
	
	public static void main(String[] args) throws Exception {
		final FileInputStream fis = new FileInputStream(args[0]);
		InputStreamLineIterator isli = new InputStreamLineIterator(fis);
		while (isli.hasNext()) {
			System.out.println(isli.next());
		}
		fis.close();
	}
}
