package iitb.CSAW.Corpus.Webaroo;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.Util;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.nutch.io.UTF8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.webaroo.ppreader.AttrParsedPage;
import com.webaroo.ppreader.ChannelReader;
import com.webaroo.ppreader.NIODataInput;
import com.webaroo.ppreader.QueueReader;

/**
 * Counts the number of documents in each of the out_data_ files supplied
 * by Webaroo. The counts are then used to assign system-wide unique long
 * IDs to each document.
 * @author soumen
 */
public class DocumentCounter implements Runnable {
	/**
	 * 
	 * @param args [0]=/path/to/log4j/conf [1]=/path/to/log/file [2]=/path/to/webaroo/dir
	 */
	public static void main(String[] args) throws Exception {
		new Config(args[0], args[1]);
		Logger logger = Util.getLogger(Object.class);
		logger.setLevel(Level.INFO);
		Logger output = Util.getLogger(DocumentCounter.class);
		ProgressLogger pl = new ProgressLogger(logger);
		pl.logInterval = 10000;
//		pl.expectedUpdates = 500000;
		pl.start();
		
		File webDirOrFile = new File(args[2]);
		if (webDirOrFile.isFile()) {
			new DocumentCounter(output, pl, webDirOrFile).run();
			return;
		}

		final int numProc = Runtime.getRuntime().availableProcessors();
		ExecutorService executor = Executors.newFixedThreadPool(numProc);
		File webFiles[] = webDirOrFile.listFiles(new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().startsWith("out_data_") && pathname.isFile();
			}
		});
		Arrays.sort(webFiles);
		
		for (File webFile : webFiles) {
			Runnable worker = new DocumentCounter(output, pl, webFile);
			executor.execute(worker);
		}
		
		executor.shutdown();
		while (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)) {
			output.info("Waiting for termination " + System.currentTimeMillis() + "[" + new Date() + "]");
		}
		
		pl.done();
	}
	
	final Logger output;
	final ProgressLogger pl;
	final File webDirOrFile;
	
	DocumentCounter(Logger output, ProgressLogger pl, File webDirOrFile) {
		this.output = output;
		this.pl = pl;
		this.webDirOrFile = webDirOrFile;
	}
	
	public void run() {
		try {
			output.info("scanning " + webDirOrFile.getName());
			NIODataInput in = new ChannelReader(new FileInputStream(webDirOrFile).getChannel(), 10 * 1024 * 1024);
			QueueReader<AttrParsedPage> reader = new QueueReader<AttrParsedPage>(in, 1);
			long discard = 0;
			for (long numDocs = 0; ; ++numDocs) {
				try {
					AttrParsedPage p = reader.readObject(AttrParsedPage.class);
					if (p == null) {
						output.warn(DocumentCounter.class.getName() + " " + webDirOrFile.getName() + " " + numDocs);
						break;
					}
					final String pageText = p.getHTML((char) 0);
					final UTF8 title = p.getTitle();
					//					final URL url = p.getPageURL();
					discard += pageText == null? 0 : pageText.length();
					discard += title == null? 0 : title.getLength();
					pl.update();
				}
				catch (Exception anyx) {
					output.warn("retrieval error on file " + webDirOrFile.getName() + " page " + numDocs + " " + anyx.getMessage());
					anyx.printStackTrace();
				}
			}
			in.close();
			output.debug("discard=" + discard);
		}
		catch (IOException iox) {
			output.warn("IOException on file " + webDirOrFile.getName());
		}
	}
}
