package iitb.CSAW.Corpus;

import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class AStripeManager {
	public synchronized static AStripeManager construct(Config conf) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		return (AStripeManager) Class.forName(conf.getString(AStripeManager.class.getSimpleName())).getConstructor(Config.class).newInstance(conf);
	}
	
	abstract public String myHostName(); 
	abstract public int numHostStripes();
	abstract public String hostStripeToName(int hostStripe);
	abstract public int hostNameToStripe(String hostName);
	abstract public int myHostStripe();

	abstract public int numDiskStripes();
	/** Each host can belong to exactly one disk stripe. */
	abstract public int hostToDiskStripe(int hostStripe);
	abstract public int myDiskStripe();
	/** In general one disk stripe maps to multiple host buddies. */
	abstract public IntList buddyHostStripes(int diskStripe);
	/** Within the buddy group associated with my disk stripe. */
	abstract public int myBuddyIndex();
	
	/** Local, no one else needs to know. */
	abstract public File myTokenIndexRunDir();
	abstract public File mySipIndexRunDir();
	public static final String tmpDirKey = "tmpDir";
	public static final String hostNameKey = "HostName";
	abstract public File getTmpDir(int hostStripe);
	
	/*
	 * We reuse URI because it can conveniently package a hostname and a path.
	 * All URIs should have scheme "ssh" and be of the form "ssh://host/path"
	 */
	protected final static String scheme = "ssh";

	/** Usually called with local diskStripe but kept generic. */
	abstract public URI corpusDir(int diskStripe) throws URISyntaxException;

	/** May return local or remote resource depending on aHostStripe. */
	abstract public URI	tokenIndexHostDir(int aHostStripe) throws URISyntaxException;
	abstract public URI sipIndexHostDir(int aHostStripe) throws URISyntaxException;
	
	/** Returns local path to mirrors of remote host. */
	abstract public File tokenIndexHostMirrorDir(int aHostStripe);
	abstract public File sipIndexHostMirrorDir(int aHostStripe);

	/**
	 * If aDiskStripe is mine, returns path to local disk. Otherwise, picks an
	 * arbitrary buddy belonging to the diskStripe and returns the path there.
	 */
	abstract public URI tokenIndexDiskDir(int aDiskStripe) throws URISyntaxException;
	abstract public URI sipIndexDiskDir(int aDiskStripe) throws URISyntaxException;

	/** Returns remote disk stripe resource at a specified remote host. */
	abstract public URI tokenIndexDiskRemoteDir(int aHostStripe) throws URISyntaxException;
	abstract public URI sipIndexDiskRemoteDir(int aHostStripe) throws URISyntaxException;
	
	abstract public boolean isMyJob(long docId);
}
