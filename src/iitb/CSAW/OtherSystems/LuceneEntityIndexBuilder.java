package iitb.CSAW.OtherSystems;

import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Catalog.YAGO.YagoCatalog;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaDocument;
import iitb.CSAW.Utils.ByteObjectInterConvertor;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Payload;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.FSDirectory;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentLockedException;

public class LuceneEntityIndexBuilder {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception  {
		final Config config = new Config(args[0], args[1]);
		final String fieldName = "ent";//args[2];
		LuceneEntityIndexBuilder wib = new LuceneEntityIndexBuilder(config, fieldName);
		final int nThreads = config.getInt(Config.nThreadsKey, Runtime.getRuntime().availableProcessors());
		wib.run(nThreads);
		wib.close();
	}

	final Config config;
	final BarcelonaCorpus corpus;
	final File indexDir;
	final AtomicInteger sharedBatchNumber = new AtomicInteger();
	final Field field = Field.token;
	final Logger logger = Util.getLogger(getClass());
	final ProgressLogger pl = new ProgressLogger(logger);
	TIntIntHashMap atypeCountMap = new TIntIntHashMap(); 
	ACatalog catalog;
	IndexWriter writer;
	
	public class WikiEntityStream extends TokenStream{
		BarcelonaDocument mdoc;
		ACatalog catalog;
		final MutableString entName = new MutableString();
		final MutableInt entPos = new MutableInt();
		IntArrayList entIdList = new IntArrayList();
		IntArrayList catList = new IntArrayList();
		IntArrayList posList = new IntArrayList();
		IntArrayList beginOffsetList = new IntArrayList();
		IntArrayList endOffsetList = new IntArrayList();
		TIntIntHashMap atypeCountMap;
		int nextTokenId = 0;
		
		public WikiEntityStream(BarcelonaDocument mdoc, ACatalog catalog, TIntIntHashMap atypeCountMap){
			this.mdoc = mdoc;
			this.atypeCountMap = atypeCountMap;
			this.catalog = catalog;
			
			// TODO Ganesh had a bug here, cannot use same WikipediaDocument to get both entity and token stream
			throw new NotImplementedException("Ganesh must migrate from WikipediaDocument to BarcelonaDocument.");
		}
		
		public Token next(Token token){
			if(nextTokenId >= catList.size()) return null;
			int prevPos = 0;
			if(nextTokenId > 0) prevPos = posList.getInt(nextTokenId-1); 
			token.setTermBuffer(catList.get(nextTokenId).toString());
			token.setStartOffset(beginOffsetList.getInt(nextTokenId));
			token.setEndOffset(endOffsetList.getInt(nextTokenId));
			token.setPositionIncrement(posList.get(nextTokenId) - prevPos);	
			byte[] payloadArray = ByteObjectInterConvertor.intToByteArray(entIdList.get(nextTokenId));
			Payload payload = new Payload(payloadArray, 0, payloadArray.length);
			token.setPayload(payload);
			//System.out.println(token);
			nextTokenId++;
			return token;
		}
	}

	public class WikiTokenStream extends TokenStream{
		BarcelonaDocument mdoc;
		final MutableString tokenString = new MutableString();
		final MutableInt tokenPos = new MutableInt();
		ArrayList<String> tokenStringList = new ArrayList<String>();
		IntArrayList posList = new IntArrayList();
		IntArrayList beginOffsetList = new IntArrayList();
		IntArrayList endOffsetList = new IntArrayList();
		int nextTokenId = 0;
		
		public WikiTokenStream(BarcelonaDocument mdoc){
			this.mdoc = mdoc;
			
			// TODO Ganesh had a bug here, cannot use same WikipediaDocument to get both entity and token stream
			throw new NotImplementedException("Ganesh must migrate from WikipediaDocument to BarcelonaDocument.");
		}
		
		public Token next(Token token){
			if(nextTokenId >= tokenStringList.size()) return null;
			int prevPos = 0;
			if(nextTokenId > 0) prevPos = posList.getInt(nextTokenId-1); 
			token.setTermBuffer(tokenStringList.get(nextTokenId));
			token.setStartOffset(beginOffsetList.getInt(nextTokenId));
			token.setEndOffset(endOffsetList.getInt(nextTokenId));
			token.setPositionIncrement(posList.get(nextTokenId) - prevPos);	
			//System.out.println(token);
			nextTokenId++;
			return token;
		}
	}

	LuceneEntityIndexBuilder(Config config, String fieldName) throws EnvironmentLockedException, IOException, DatabaseException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		this.config = config;
		this.corpus = new BarcelonaCorpus(config);
		this.indexDir = new File(config.getString(iitb.CSAW.OtherSystems.PropertyKeys.indexDirKey));
		catalog = YagoCatalog.getInstance(config); 
		writer = new IndexWriter(FSDirectory.getDirectory(/*"/mnt/b100/d0/ganesh/bag/lucene-index"*/indexDir), new WhitespaceAnalyzer(), MaxFieldLength.UNLIMITED);
		
		/**Loading queries and registering all the unique entityIDs and their counts in an RBTreemap*/
		BufferedReader br = new BufferedReader(new FileReader("/mnt/b100/d0/devshree/queries/atypes.sorted"));
		String line = null;
		while((line=br.readLine())!=null){
			line.trim();
			int catId = catalog.catNameToCatID(line);
			atypeCountMap.adjustOrPutValue(catId,1,1);
		}
		System.out.println(atypeCountMap);
		/*HashMap<String, RootQuery> queries  = new HashMap<String, RootQuery>();
		File qDir = new File(config.getString(iitb.CSAW.EntityRank.Wikipedia.PropertyKeys.batchQueryDirKey));
		final int slop = config.getInt(PropertyKeys.windowKey);
		QueryWithAnswers.loadQueries2(qDir, logger, slop, queries);
		for (RootQuery query : queries.values()) {
			for (ContextQuery contextQuery : query.contexts) {
				for (MatcherQuery matcherQuery : contextQuery.matchers) {
					if (matcherQuery instanceof TypeBindingQuery) {
						final TypeBindingQuery typeBindingQuery = (TypeBindingQuery) matcherQuery;
						int typeId = catalog.catNameToCatID(typeBindingQuery.typeName);
						System.out.println(typeBindingQuery.typeName+","+typeId);
						atypeCountMap.adjustOrPutValue(typeId,1,1);
					}
				}
			}
		}*/
	}
	
	void close() throws IOException, DatabaseException {
		corpus.close();
		writer.optimize();
		writer.close();
	}
	
	class Worker implements Runnable {
		@Override
		public void run() {
			try {
				BarcelonaDocument mdoc = new BarcelonaDocument();
				while (corpus.nextDocument(mdoc)) {
					sharedBatchNumber.getAndIncrement();
					Document doc = new Document();
					WikiEntityStream wikiEntityStream = new WikiEntityStream(mdoc, catalog, atypeCountMap);
					WikiTokenStream wikiTokenStream = new WikiTokenStream(mdoc);
					doc.add(new org.apache.lucene.document.Field("categories", wikiEntityStream));
					doc.add(new org.apache.lucene.document.Field("tokens", wikiTokenStream));
					writer.addDocument(doc);
					pl.update();
				}
			}
			catch (Exception anyex) {
				logger.fatal(anyex);
				anyex.printStackTrace();
			}
		}
	}
	
	void run(int numThreads) throws Exception {
		sharedBatchNumber.set(0);
		pl.displayFreeMemory = true;
		pl.expectedUpdates = corpus.numDocuments();
		corpus.reset();
		pl.start("indexing");
		
		if (numThreads == 1) {
			new Worker().run();
		}
		else {
			ArrayList<Thread> workers = new ArrayList<Thread>();
			for (int tx = 0; tx < numThreads; ++tx) {
				Thread worker = new Thread(new Worker(), "Worker" + tx);
				workers.add(worker);
				worker.start();
			}
		}
		
		pl.done();
		
	}
}
