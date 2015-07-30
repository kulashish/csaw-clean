package iitb.CSAW.Corpus.Webaroo;

import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.WorkerPool;
import iitb.CSAW.Utils.IWorker;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.mg4j.tool.Scan;
import it.unimi.dsi.parser.BulletParser;
import it.unimi.dsi.parser.callback.TextExtractor;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Iterator;
import java.util.List;

import net.nutch.io.UTF8;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jdom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sleepycat.je.DatabaseException;
import com.webaroo.ppreader.AttrParsedPage;
import com.webaroo.ppreader.ChannelReader;
import com.webaroo.ppreader.NIODataInput;
import com.webaroo.ppreader.QueueReader;

/**
 * Turns Webaroo raw corpus files into RAR format, one repository per disk stripe.
 * 2011/05/08 soumen Removed the ability to run on all hosts and partition a
 * disk stripe's data across buddies belonging to that disk stripe.  Runs only on
 * buddy leaders now, processing the whole disk stripe.
 * 
 * @author soumen
 */
public class CorpusBuilder {
	/**
	 * @param args [0]=/path/to/properties [1]=/path/to/log/file
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		CorpusBuilder cb = new CorpusBuilder(config);
		cb.build();
		cb.close();
	}
	
	static final long LOG_INTERVAL = ProgressLogger.ONE_MINUTE * 5;
	static final int BUF_SIZE = (1<<10)*256;

	final Config config;
	final Logger logger;
	final WebarooStripeManager stripeManager;
	final File rawSourceDir, corpus1Dir;
	final WebarooCorpus corpus1;
	
	CorpusBuilder(Config config) throws Exception {
		this.config = config;
		stripeManager = new WebarooStripeManager(config);
		logger = Logger.getLogger(getClass());
		// stripe manager will ensure exclusion so we do not need to name apart target paths for buddies
		rawSourceDir = new File(config.getString(PropertyKeys.rawSourcePattern).replaceAll("\\$\\{HostName\\}", stripeManager.myHostName()));
		final URI myCorpus1Uri = stripeManager.corpusDir(stripeManager.myDiskStripe());
		assert myCorpus1Uri.getHost().equals(stripeManager.myHostName());
		corpus1Dir = new File(myCorpus1Uri.getPath());
		corpus1 = new WebarooCorpus(config, corpus1Dir, true, true); // note: TRUNCATE
	}
	
	void close() throws IOException, DatabaseException {
		corpus1.close();
	}
	
	void build() throws Exception {
		if (stripeManager != null && stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Run this only on buddy leaders.");
			return;
		}
		final int numProc = config.getInt(Config.nThreadsKey);
		final WorkerPool workerPool = new WorkerPool(this, numProc);
		build(workerPool, rawSourceDir);
		workerPool.pollToCompletion(ProgressLogger.ONE_MINUTE, null);
	}

	void build(WorkerPool executor, File wDirOrFile) {
		if (wDirOrFile.isDirectory()) {
			for (File wSub : wDirOrFile.listFiles()) {
				build(executor, wSub);
			}
		}
		else if (wDirOrFile.getName().startsWith("out_data_")){
			// all out_data files found in my raw corpus dir is my work
			final Worker worker = new Worker(wDirOrFile);
			executor.add(worker);
			//			worker.run();
		}
	}

	/**
	 * 2010-06-08 Tried Neko, JTidy, and Webaroo's HTML cleaner
	 * and text extractor, then selected MG4J's implementation.
	 */
	static boolean transcribe(long docId, AttrParsedPage page, MutableString plainText, MutableString word, MutableString nonWord, IntArrayList offsets, WebarooDocument wdoc) {
		final URL url = page.getPageURL();
		final UTF8 title = page.getTitle();
		final String pageTextHTML = page.getHTML((char) 0);
		BulletParser bp = new BulletParser();
		TextExtractor te = new TextExtractor();
		bp.setCallback(te);
		bp.parse(pageTextHTML.toCharArray());
		
		plainText.setLength(0);
		// append both titles in case one fails
		if (title != null) {
			plainText.append(title.toString());
		}
		plainText.append(" ||| ");
		plainText.append(te.title);
		plainText.append(" ||| ");
		plainText.append(te.text);
		squeezeSpace(plainText);
		// tokenize
		offsets.size(0);
//		DelimitedWordReader tokenizer = new DelimitedWordReader(plainText, DELIMITERS);
		FastBufferedReader tokenizer = new FastBufferedReader(plainText);
		/*
		 * FastBufferedReader is safer than DelimitedWordReader with custom delimiters.
		 * It is possible to define delimiters such that the UTF8 termMap writers will bomb. 
		 */
		try {
			for (int chofs=0; ;) {
				if (!tokenizer.next(word, nonWord)) {
					break;
				}
				chofs += word.length();
				offsets.add(chofs);
				chofs += nonWord.length();							
				offsets.add(chofs);
			} // for-token
		}
		catch (IOException iox) {
			return false;
		}
		wdoc.assign(docId, url, title, plainText, offsets);
		return true;
	}
	
	static void squeezeSpace(MutableString ms) {
		final char[] buf = ms.array();
		final int oldLen = ms.length();
		boolean lastWasSpace = false;
		int nx = 0, ox = 0;
		for (; ox < oldLen; ++ox) {
			final boolean thisIsSpace = Character.isWhitespace(buf[ox]); 
			if (!thisIsSpace || !lastWasSpace) {
				buf[nx++] = buf[ox];
				lastWasSpace = thisIsSpace; 
			}
		}
		ms.setLength(nx);
		ms.changed();
	}
	
	class Worker implements IWorker {
		final ProgressLogger pl;
		final File srcFile;
		final long beginDocId, numDocs;
		final FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream(BUF_SIZE);
		
		Worker(File srcFile) {
			this.srcFile = srcFile;
			this.beginDocId = stripeManager.beginDocId(srcFile);
			this.numDocs = stripeManager.numDocs(srcFile);
			this.pl = new ProgressLogger(logger, LOG_INTERVAL, srcFile.getName());
			pl.priority = Level.DEBUG;
			pl.expectedUpdates = this.numDocs;
			pl.displayFreeMemory = true;
		}
		
		@Override 
		public Exception call() throws Exception {
			try {
				pl.start();
				logger.warn("Converting " + srcFile + " [" + beginDocId + "]");
				NIODataInput in = new ChannelReader(new FileInputStream(srcFile).getChannel(), 10 * 1024 * 1024);
				QueueReader<AttrParsedPage> reader = new QueueReader<AttrParsedPage>(in, 1);
				MutableString plainText = new MutableString(Scan.DEFAULT_BUFFER_SIZE);
				MutableString word = new MutableString(), nonWord = new MutableString();
				IntArrayList offsets = new IntArrayList();
				final WebarooDocument wdoc = new WebarooDocument();
				long numDocs = 0, numErrors = 0, numWords = 0, lastPrint = System.currentTimeMillis();
				for ( ; ; ++numDocs) {
					try {
						final AttrParsedPage p = reader.readObject(AttrParsedPage.class);
						if (p == null) {
							break;
						}
						final long docid = beginDocId + numDocs;
						transcribe(docid, p, plainText, word, nonWord, offsets, wdoc);
						corpus1.appendDocument(wdoc, fbaos);
						logger.trace(docid + " " + wdoc.url + " " + wdoc);
						numWords += offsets.size();
					}
					catch (Exception anyx) {
						logger.warn("error on " + srcFile + " page " + numDocs + " docid " + (beginDocId+numDocs) + " [" + anyx.getMessage() + "]");
//						anyx.printStackTrace();
						++numErrors;
					}
					pl.update();
					final long now = System.currentTimeMillis();
					if (now - lastPrint > LOG_INTERVAL) {
						logger.debug(srcFile.getName() + " " + numDocs + " docs " + numWords + " words " + String.format("%g", 1d * numWords / (1d+numDocs)) + " words/doc");
						lastPrint = now;
					}
					
				} // for-doc
				in.close();
				logger.warn("Finished " + srcFile + " " + numDocs + " docs " + numErrors + " errors " + numWords + " words " + String.format("%g", 1d * numWords / (1d+numDocs)) + " words/doc");
				pl.stop();
				pl.done();
			}
			catch (IOException iox) {
				logger.error("problem with " + srcFile, iox);
			}
			return null;
		}
		
		void listChildren(Node node, MutableString ms) {
			if (node == null) return;
			if (node.getNodeType() == Node.TEXT_NODE) {
				ms.append(node.getNodeValue());
			} else if (node.hasChildNodes()) {
				NodeList childList = node.getChildNodes();
				final int childLen = childList.getLength();
				for (int count = 0; count < childLen; count ++)
					listChildren(childList.item(count), ms);
			}
			else return;
		}
		
		@SuppressWarnings("unchecked")
		public void listChildren(Element current, MutableString outPlainText) {
			final String textElement = (current.getText() + "\t").trim();
			outPlainText.append(textElement);
			final List<Object> children = current.getChildren();
			for (Iterator<Object> iterator = children.iterator(); iterator.hasNext(); ) {
				Element child = (Element) iterator.next();
				listChildren(child, outPlainText);
			}
		}

		@Override
		public long numDone() {
			return pl.count;
		}
	}
}
