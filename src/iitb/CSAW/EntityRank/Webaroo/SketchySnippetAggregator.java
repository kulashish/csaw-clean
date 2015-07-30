package iitb.CSAW.EntityRank.Webaroo;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import iitb.CSAW.Utils.Sort.BitExternalMergeReduce;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * {@link SketchySnippet}s are saved over all stripes.
 * This class aggregates these records. First, within a stripe,
 * then, across stripes.
 * 
 * @author soumen
 */
public class SketchySnippetAggregator<T extends IBitReducible<T>> {
	/**
	 * @param args [0]=config [1]=log [2]=/path/to/snippets [3]=op
	 */
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]);
		File snDir = new File(args[2]);
		if (args[3].equals("local")) {
			{
				SketchySnippetAggregator<SketchySnippet.QEWC> ssa = new SketchySnippetAggregator<SketchySnippet.QEWC>(conf, SketchySnippet.QEWC.class);
				ssa.aggregateLocal(snDir);
			}
			{
				SketchySnippetAggregator<SketchySnippet.QEN> ssa = new SketchySnippetAggregator<SketchySnippet.QEN>(conf, SketchySnippet.QEN.class);
				ssa.aggregateLocal(snDir);
			}
		}
		else if (args[3].equals("global")) {
			{
				SketchySnippetAggregator<SketchySnippet.QEWC> ssa = new SketchySnippetAggregator<SketchySnippet.QEWC>(conf, SketchySnippet.QEWC.class);
				ssa.aggregateGlobal(snDir);
			}
			{
				SketchySnippetAggregator<SketchySnippet.QEN> ssa = new SketchySnippetAggregator<SketchySnippet.QEN>(conf, SketchySnippet.QEN.class);
				ssa.aggregateGlobal(snDir);
			}
		}
	}

	final Logger logger = Logger.getLogger(getClass());
	final Config conf;
	final File tmpDir;
	final Class<T> type;
	final AStripeManager asm;
	
	public SketchySnippetAggregator(Config conf, Class<T> type) throws Exception {
		this.conf = conf;
		asm = AStripeManager.construct(conf);
		tmpDir = asm.getTmpDir(asm.myHostStripe());
		this.type = type;
	}

	private void aggregateGlobal(File snDir) throws InstantiationException, IllegalAccessException, IOException {
		ReferenceArrayList<File> inRunFiles = new ReferenceArrayList<File>();
		for (File candFile : snDir.listFiles()) {
			if (candFile.getName().matches(type.getCanonicalName() + "_\\d+\\.dat")) {
				inRunFiles.add(candFile);
			}
		}
		logger.debug("Globally reducing " + inRunFiles);
		BitExternalMergeReduce<T, T> bemr = new BitExternalMergeReduce<T, T>(type, type, tmpDir);
		bemr.reduceUsingHeap(inRunFiles, type.newInstance().getReducer(), new File(snDir, type.getCanonicalName() + ".dat"));
	}

	private void aggregateLocal(File snDir) throws InstantiationException, IllegalAccessException, IOException {
		ReferenceArrayList<File> inRunFiles = new ReferenceArrayList<File>();
		for (File candFile : snDir.listFiles()) {
			if (candFile.getName().startsWith(type.getCanonicalName() + "_" + asm.myDiskStripe() + "_")) {
				inRunFiles.add(candFile);
			}
		}
		logger.debug("Locally reducing " + inRunFiles);
		BitExternalMergeReduce<T, T> bemr = new BitExternalMergeReduce<T, T>(type, type, tmpDir);
		bemr.reduceUsingHeap(inRunFiles, type.newInstance().getReducer(), new File(snDir, type.getCanonicalName() + "_" + asm.myDiskStripe() + ".dat"));
	}
}
