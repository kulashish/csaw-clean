package iitb.CSAW.Utils.Sort;

import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.lang.MutableString;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.BasicConfigurator;

public class MutableStringRecord extends MutableString implements IRecord {
	private static final long serialVersionUID = 1L;

	@Override
	public void load(DataInput dii) throws IOException {
		readSelfDelimUTF8(dii);
	}

	@Override
	public void store(DataOutput doi) throws IOException {
		writeSelfDelimUTF8(doi);
	}

	public void replace(IRecord o) {
		super.replace((MutableString) o);
	}

	public static class Comparator implements java.util.Comparator<MutableStringRecord> {
		@Override
		public int compare(MutableStringRecord o1, MutableStringRecord o2) {
			return o1.compareTo(o2);
		}
	}

	public static Iterable<String> getIterable(final File file) throws FileNotFoundException {
		return new Iterable<String>() {
			@Override
			public Iterator<String> iterator() {
				try {
					final DataInputStream dis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(file)));
					final MutableStringRecord msr = new MutableStringRecord();
					msr.load(dis);
					
					return new Iterator<String>() {
						String record = msr.toString();
						
						@Override
						public boolean hasNext() {
							return record != null;
						}

						@Override
						public String next() {
							final String ans = record;
							try {
								msr.load(dis);
								record = msr.toString();
							} catch (IOException e) {
								record = null;
							}
							return ans;
						}
						
						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}
					};
				} catch (IOException e) {
					return null;
				}
			}
		};
	}

	
	/**
	 * Test harness for {@link ExternalMergeSort}.
	 */
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		List<String> inp = Arrays.asList("mary", "had", "a", "little", "lamb", "and", "i", "had", "eaten", "it");
//		List<String> inp = Arrays.asList("lamb", "and", "i", "had");
		ArrayList<MutableStringRecord> inp2 = new ArrayList<MutableStringRecord>();
		for (String inpx : inp) {
			MutableStringRecord lr = new MutableStringRecord();
			lr.replace(inpx);
			inp2.add(lr);
		}
		File tmpDir = new File(System.getProperty("java.io.tmpdir"));
		ExternalMergeSort<MutableStringRecord> ems = new ExternalMergeSort<MutableStringRecord>(MutableStringRecord.class, new Comparator(), true, tmpDir);
		ems.setRunSize(3);
		File outFile = new File(tmpDir, ems.getClass().getSimpleName() + ".dat");
		ems.runFanIn(inp2, outFile);
		DataInputStream outDis = new DataInputStream(new FileInputStream(outFile));
		try {
			for (MutableStringRecord lr = new MutableStringRecord();;) {
				lr.load(outDis);
				System.out.println(lr);
			}
		}
		finally {
			outDis.close();
		}
	}
}
