package iitb.CSAW.EntityRank.Wikipedia;

import gnu.trove.TIntIntHashMap;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus;
import iitb.CSAW.Corpus.Wikipedia.BarcelonaDocument;
import iitb.CSAW.EntityRank.InexTrec.QueryWithAnswers;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Search.ProbeQueryProcessor;
import iitb.CSAW.Search.ScanQueryProcessor;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import it.unimi.dsi.mg4j.index.TermProcessor;

import java.util.Collections;
import java.util.HashMap;

import org.apache.log4j.Logger;

public class ScanProbeQueryTest {
	public static final void main(String[] args) throws Exception {
		mainScanProbeCompare(args);
	}
	
	static final Logger logger = Logger.getLogger(ScanProbeQueryTest.class);
	
	/** Compare scan and probe for discrepancies. */
	static void mainScanProbeCompare(String[] args) throws Exception {
		Config config = new Config(args[0], args[1]);
		ACatalog catalog = ACatalog.construct(config);
		final TermProcessor termProcessor = DefaultTermProcessor.construct(config);
		HashMap<String, QueryWithAnswers> queries = new HashMap<String, QueryWithAnswers>();
		QueryWithAnswers.load(config, queries);
		ProbeQueryProcessor pqp = new ProbeQueryProcessor(config);
		final ReferenceArrayList<ContextWitness> cwitsProbe = new ReferenceArrayList<ContextWitness>();

		final BarcelonaCorpus corpus = new BarcelonaCorpus(config);
		final BarcelonaDocument doc = new BarcelonaDocument();
		final ReferenceArrayList<ContextWitness> cwitsScan = new ReferenceArrayList<ContextWitness>();
		final ScanQueryProcessor sqp = new ScanQueryProcessor(config);

		for (QueryWithAnswers qwa : queries.values()) {
			final int atypeCatID = catalog.catNameToCatID(qwa.bindingType());
			if (atypeCatID == -1) {
				logger.warn(qwa.rootQuery.queryId + " has no recognized atype [" + qwa.bindingType() + "]");
				continue;
			}
			logger.info("executing " + qwa.rootQuery);
			pqp.execute(qwa.rootQuery, cwitsProbe);
			logger.info(cwitsProbe.size() + " context witnesses from probe");
			// collect docids
			final TIntIntHashMap docIdToNumSnippets = new TIntIntHashMap();
			for (ContextWitness cwit : cwitsProbe) {
				docIdToNumSnippets.adjustOrPutValue(cwit.docId, 1, 1);
			}
			// Check if number of witnesses per document remains the same across scan and probe.
			for (int docId : docIdToNumSnippets.keys()) {
				final boolean found = corpus.getDocument(docId, doc);
				assert found;
				cwitsScan.clear();
				sqp.execute(qwa.rootQuery, doc, termProcessor.copy(), cwitsScan);
				// equality of size does not mean witness set equality...
				if (docIdToNumSnippets.get(docId) != cwitsScan.size()) {
					logger.fatal(qwa.rootQuery.queryId + " " + docId + " " + docIdToNumSnippets.get(docId) + " " + cwitsScan.size());
					ReferenceArrayList<ContextWitness> cwitsProbeSubset = new ReferenceArrayList<ContextWitness>();
					for (ContextWitness cw : cwitsProbe) {
						if (cw.docId == docId) {
							cwitsProbeSubset.add(cw);
						}
					}
					Collections.sort(cwitsProbeSubset);
					logger.fatal(qwa.rootQuery.queryId + " " + docId + " " + cwitsProbeSubset.size() + " " + cwitsScan.size());
				}
				else {
					logger.debug(qwa.rootQuery.queryId + " " + docId + " ok");
				}
			}
		}
	}
	
}
