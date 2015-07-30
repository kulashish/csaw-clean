package iitb.CSAW.Spotter;

import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectIterator;
import iitb.CSAW.Catalog.ACatalog;
import iitb.CSAW.Corpus.DefaultTermProcessor;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.bytes.ByteList;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Building the trie is not thread-safe. Reading it for scanning text should be.
 * Would be nicer to separate the basic trie and what payload is associated with
 * leaf nodes (here, sets of entity IDs).
 * 
 * @author soumen
 */
public class MentionTrie {
	/**
	 * Used to make concurrent read access safe.
	 */
	public static class KeyMaker {
		final byte[] buf = new byte[DefaultTermProcessor.MAX_TERM_LENGTH * Long.SIZE];
		final FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream(buf);
		final DataOutputStream dos = new DataOutputStream(fbaos);
		private final ByteArrayList tmpBal = new ByteArrayList();
		
		public void makeKey(int nodeId, String token, ByteArrayList outBal) throws IOException {
			fbaos.reset();
			dos.writeInt(nodeId);
			dos.writeChars(token);
			dos.flush();
			fbaos.flush();
			outBal.addElements(outBal.size(), buf, 0, fbaos.length);
			assert outBal.size() > 0;
		}
	}
	
	private static MentionTrie _inst = null;

	static final Logger logger = Logger.getLogger(MentionTrie.class);
	public static final int rootNodeId = -1;
	private int internalNodeIdGen = rootNodeId-1; // dec after use
	/** <b>Note</b> that leaf node IDs are assigned depending on the order
	 * in which phrases are presented to the trie in the constructor. */
	private int leafNodeIdGen = 0;
	final KeyMaker keyMaker = new KeyMaker();
	final ByteArrayList sharedKeyBuf = new ByteArrayList();
	/** Using a hash table here means it's painful to print a traversal of the trie. */
	final Object2IntMap<ByteList> nodeIdTokenToKidId = new Object2IntOpenHashMap<ByteList>();
	/**
	 * The ent lists will include {@link Spot#naEnt} and be sorted in 
	 * increasing ent order at the end of the load.
	 */
	final Int2ObjectMap<IntList>  leafNodeIdToEntIds = new Int2ObjectOpenHashMap<IntList>();
	final TIntObjectHashMap<IntList> entToLeaves = new TIntObjectHashMap<IntList>();
	
	private void put(int entId, List<String> mentionTokens) throws IOException {
		int curNodeId = rootNodeId;
		for (int tx = 0, tn = mentionTokens.size(); tx < tn; ++tx) {
			final String token = mentionTokens.get(tx);
			final int preSize = sharedKeyBuf.size();
			keyMaker.makeKey(curNodeId, token, sharedKeyBuf);
			final int postSize = sharedKeyBuf.size();
			final ByteList byteListKey = sharedKeyBuf.subList(preSize, postSize);
			if (nodeIdTokenToKidId.containsKey(byteListKey)) {
				final int prevNodeId = curNodeId;
				curNodeId = nodeIdTokenToKidId.get(byteListKey);
				logger.trace("hit " + prevNodeId + "," + token + " --> " + curNodeId);
				sharedKeyBuf.size(preSize); // erase
			}
			else {
				final int prevNodeId = curNodeId;
				curNodeId = (tx == tn-1)? leafNodeIdGen++ : internalNodeIdGen--;
				logger.trace("miss " + prevNodeId + "," + token + " --> " + curNodeId);
				nodeIdTokenToKidId.put(byteListKey, curNodeId);
			}
		}
		final IntList target;
		if (leafNodeIdToEntIds.containsKey(curNodeId)) {
			target = leafNodeIdToEntIds.get(curNodeId);
		}
		else {
			target = new IntArrayList();
			leafNodeIdToEntIds.put(curNodeId, target);
		}
		if (!target.contains(entId)) {
			target.add(entId);
			logger.trace("leaf " + curNodeId + " += " + entId);
		}
	}
	
	private void addNaSortEntsAtLeaves() {
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = leafNodeIdToEntIds.size();
		pl.start("Sorting ents at leaves.");
		for (Map.Entry<Integer, IntList> l2ex : leafNodeIdToEntIds.entrySet()) {
			l2ex.getValue().add(Spot.naEnt);
			Collections.sort(l2ex.getValue());
			l2ex.setValue(IntLists.unmodifiable(l2ex.getValue()));
			pl.update();
		}
		pl.stop();
		pl.done();
	}
	
	/**
	 * @param km to make lookups thread-safe
	 * @param nodeId current node ID
	 * @param token
	 * @return child node ID if token is an out-edge from nodeId, otherwise 
	 * just nodeId to signify end-of-path
	 * @throws IOException
	 */
	public int step(KeyMaker km, int nodeId, String token) throws IOException {
		km.tmpBal.clear();
		km.makeKey(nodeId, token, km.tmpBal);
		if (nodeIdTokenToKidId.containsKey(km.tmpBal)) {
			return nodeIdTokenToKidId.get(km.tmpBal);
		}
		else {
			return nodeId;
		}
	}
	
	public int getNumLeaves() {
		return leafNodeIdToEntIds.size();
	}
	
	public IntList getSortedEntsNoNa(int maybeLeaf) {
		if (!leafNodeIdToEntIds.containsKey(maybeLeaf)) {
			return null;
		}
		else {
			final IntList entsNa = leafNodeIdToEntIds.get(maybeLeaf);
			return entsNa.subList(1, entsNa.size());
		}
	}
	
	public IntList getSortedEntsNa(int maybeLeaf) {
		if (!leafNodeIdToEntIds.containsKey(maybeLeaf)) {
			return null;
		}
		else {
			return leafNodeIdToEntIds.get(maybeLeaf);
		}
	}
	
	public IntList getSortedLeavesForEnt(int ent) {
		if (!entToLeaves.containsKey(ent)) return null;
		return entToLeaves.get(ent);
	}

	/**
	 * Initializes an empty trie.  Internal use only.
	 */
	private MentionTrie() { }
	
	public synchronized static MentionTrie getInstance(Config conf) throws IllegalArgumentException, SecurityException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException {
		if (_inst == null) {
			final ACatalog catalog = ACatalog.construct(conf);
	    	final File cleanedMentionsFile = new File(conf.getString(iitb.CSAW.Spotter.PropertyKeys.cleanedMentionsFileName));
	    	_inst = new MentionTrie(catalog, cleanedMentionsFile);
		}
		return _inst;
	}
	
	private MentionTrie(ACatalog catalog, File oneMentionFile) throws IOException {
		this(catalog, oneMentionFile, null);
	}
	
	public MentionTrie(ACatalog catalog, File oneMentionFile, Level level) throws IOException {
		this(catalog, Arrays.asList(oneMentionFile), level);
	}

	/**
	 * @param catalog
	 * @param mentionFiles A list of files containing {@link MentionRecord} records.
	 * Leaf node IDs are assigned from 0 onward in the order in which phrases are
	 * found in these files.  Internal nodes are assigned negative IDs starting
	 * at -1 for the root and counting down.  Redundancy across these files is 
	 * inefficient but does not affect correctness. All but the first occurrence
	 * are ignored.
	 * @throws IOException 
	 */
	public MentionTrie(ACatalog catalog, List<File> mentionFiles, Level level) throws IOException {
		final Level oldLevel = logger.getLevel();
		if (level != null) logger.setLevel(level);
		int nDiscardedMentions = 0;
		for (File mentionFile : mentionFiles) {
			DataInputStream mdis = new DataInputStream(new FastBufferedInputStream(new FileInputStream(mentionFile)));
			MentionRecord mention = new MentionRecord();
			ArrayList<String> mentionTokens = new ArrayList<String>();
			for (;;) {
				try {
					mention.load(mdis);
					final int entId = catalog.entNameToEntID(mention.entName.toString()); 
					if (entId < 0) {
						++nDiscardedMentions;
						continue;
					}
					mentionTokens.clear();
					final int nTokens = mention.size();
					for (int tx = 0; tx < nTokens; ++tx) {
						final String mentionToken = mention.token(tx).toString();
						mentionTokens.add(mentionToken);
					}
					logger.debug(mentionTokens);
					put(entId, mentionTokens);
					debugProbe(mentionTokens, mention.entName, entId);
				}
				catch (EOFException eofx) {
					break;
				}
			}
			mdis.close();
		}
		logger.setLevel(oldLevel);
		addNaSortEntsAtLeaves();
		invertLeafEntToEntLeaf(catalog);
		logger.info("Trie loaded " + mentionFiles + " mentions into " + nodeIdTokenToKidId.size() + " keys, " + getNumLeaves() + " leaves, " + nDiscardedMentions + " discarded.");
	}
	
	private void invertLeafEntToEntLeaf(ACatalog catalog) {
		final ProgressLogger pl = new ProgressLogger(logger);
		pl.expectedUpdates = leafNodeIdToEntIds.size();
		pl.start("Started inverting L2E to E2L.");
		int numLe = 0;
		for (Map.Entry<Integer, IntList> l2ex : leafNodeIdToEntIds.entrySet()) {
			final int leaf = l2ex.getKey();
			for (int ent : l2ex.getValue()) {
				final IntList target;
				if (entToLeaves.containsKey(ent)) {
					target = entToLeaves.get(ent);
				}
				else {
					target = new IntArrayList();
					entToLeaves.put(ent, target);
				}
				target.add(leaf); // no need to check for duplicates because l,e pairs are unique
				++numLe;
			}
			pl.update();
		}
		pl.stop("Finished inverting L2E to E2L.");
		pl.done();
		for (TIntObjectIterator<IntList> e2lx = entToLeaves.iterator(); e2lx.hasNext(); ) {
			e2lx.advance();
			final IntList leaves = e2lx.value(); 
			Collections.sort(leaves);
			for (int prevLeaf = Integer.MIN_VALUE, lx = 0, ln = leaves.size(); lx < ln; ++lx) {
				final int leaf = leaves.getInt(lx);
				if (prevLeaf == leaf) {
					throw new IllegalStateException("E" + e2lx.key() + " dup " + leaf);
				}
				prevLeaf = leaf;
			}
		}
	}

	private void debugProbe(List<String> mentionTokens, CharSequence entName, int entId) throws IOException {
		if (logger.getLevel() == null || !logger.getLevel().isGreaterOrEqual(Level.TRACE)) return;
		final MutableString logLine = new MutableString();
		final KeyMaker km = new KeyMaker();
		int trieNode = rootNodeId;
		for (String token : mentionTokens) {
			final int nextNode = step(km, trieNode, token);
			logLine.append("\"" + token + "\" ");
			final boolean isLeaf = leafNodeIdToEntIds.containsKey(nextNode);
			logLine.append(isLeaf? 'L' : 'N');
			logLine.append(nextNode);
			if (isLeaf) {
				logLine.append("[" + leafNodeIdToEntIds.get(nextNode).size() + "]");
			}
			logLine.append(' ');
			trieNode = nextNode;
		}
		logLine.append("E" + entId + "=" + entName);
		logger.trace(logLine);
	}
	
	public static void main(String[] args) throws Exception {
		Config conf = new Config(args[0], args[1]); // for logging
		MentionTrie.logger.setLevel(Level.DEBUG);
		MentionTrie.getInstance(conf);
	}
	
	/**
	 * Test harness.
	 * @param args
	 * @throws Exception
	 */
	public static void main2(String[] args) throws Exception {
		new Config(args[0], args[1]); // for logging
		MentionTrie emt = new MentionTrie();
		MentionTrie.logger.setLevel(Level.TRACE);
		emt.put(1001, Arrays.asList("new", "york"));
		emt.put(1003, Arrays.asList("new", "york"));
		emt.put(1002, Arrays.asList("new", "york", "times"));
		emt.put(1002, Arrays.asList("new", "york", "times")); // should have no effect
		emt.put(1001, Arrays.asList("new", "haven"));
		emt.addNaSortEntsAtLeaves();
		int nodeId = rootNodeId;
		KeyMaker km = new KeyMaker();
		System.out.println((nodeId = emt.step(km, nodeId, "new")) + " " + emt.getSortedEntsNoNa(nodeId));
		System.out.println((nodeId = emt.step(km, nodeId, "york")) + " " + emt.getSortedEntsNoNa(nodeId));
		System.out.println((nodeId = emt.step(km, nodeId, "times")) + " " + emt.getSortedEntsNoNa(nodeId));
	}
}
