package iitb.CSAW.Index;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Corpus.ACorpus.Field;
import iitb.CSAW.Index.SIP2.Sip2IndexMerger;
import iitb.CSAW.Utils.RemoteData;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.mg4j.index.DiskBasedIndex;
import it.unimi.dsi.mg4j.index.Index;
import it.unimi.dsi.util.Properties;

import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

/**
 * Methods shared between {@link TokenIndexMerger}, {@link Sip2IndexMerger},
 * and follow-ups, mostly around {@link RemoteData} usage.
 * 
 * @author soumen
 */
public class BaseIndexMerger {
	public static final int GAMMA_OFFSET = 1;
	
	final Logger logger = Logger.getLogger(getClass());

	protected void mergeDocsOccsAcrossDiskStripes(AStripeManager sm, Field field) throws Exception {
		final URI myIndexDiskUri = chooseIndexDiskUri(sm.myDiskStripe(), sm, field);
		assert myIndexDiskUri.getHost().equals(sm.myHostName());
		final Properties myProperties = new Properties(new File(myIndexDiskUri.getPath(), field + DiskBasedIndex.PROPERTIES_EXTENSION));
		int myMaxDoc1 = myProperties.getInt(Index.PropertyKeys.DOCUMENTS);
		long myNumOcc = myProperties.getLong(Index.PropertyKeys.OCCURRENCES);
		logger.info("Before stripe " + sm.myDiskStripe() + " maxDoc1 " + myMaxDoc1 + " numOcc " + myNumOcc);
		for (int aDiskStripe = 0; aDiskStripe < sm.numDiskStripes(); ++aDiskStripe) {
			if (aDiskStripe == sm.myDiskStripe()) {
				continue; // this host already taken care of above
			}
			final URI aIndexDiskUri = chooseIndexDiskUri(aDiskStripe, sm, field);
			final RemoteData rd = new RemoteData(aIndexDiskUri.getHost());
			try {
				final String remPath = aIndexDiskUri.getPath() + File.separator + field + DiskBasedIndex.PROPERTIES_EXTENSION;
				final Properties aProps = new Properties();
				final InputStream remIs = rd.getRemoteFileInputStream(remPath);
				aProps.load(remIs);
				remIs.close();
				final int otherMaxDoc1 = aProps.getInt(Index.PropertyKeys.DOCUMENTS);
				final long otherNumOcc = aProps.getLong(Index.PropertyKeys.OCCURRENCES);
				logger.info("During stripe " + aDiskStripe + " maxDoc1 " + otherMaxDoc1 + " numOcc " + otherNumOcc);
				myMaxDoc1 = Math.max(myMaxDoc1, otherMaxDoc1);
				myNumOcc += otherNumOcc;
			}
			finally {
				rd.close();
			}
		}
		logger.info("After stripe " + sm.myDiskStripe() + " maxDoc1 " + myMaxDoc1 + " numOcc " + myNumOcc);
		myProperties.addProperty(iitb.CSAW.Index.PropertyKeys.globalMaxDocument1, myMaxDoc1);
		myProperties.addProperty(iitb.CSAW.Index.PropertyKeys.globalNumOccurrences, myNumOcc);
		myProperties.save(); // must be local disk
	}

	/**
	 * To be called only by leader of buddy set.
	 * {@link RemoteData#rsyncPull(File, String)}s remote token index host
	 * directories to local disk in preparation for {@link #mergeHostsToDiskStripe()}.
	 * Note that this method will <b>always</b> wipe out the local directory
	 * if it exists, and pull the remote files afresh (which takes a while)
	 * without checking if they are complete or valid.
	 */
	protected boolean syncPullToLeader(AStripeManager stripeManager, Field field) throws Exception {
		if (stripeManager != null && stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Should run this only on buddy master.");
			return false;
		}
		final IntList buddies = stripeManager.buddyHostStripes(stripeManager.myDiskStripe());  
		for (int aHostStripe : buddies) {
			if (aHostStripe == stripeManager.myHostStripe()) continue; // exclude myself
			final File aIndexHostMirrorDir = chooseIndexHostMirrorDir(aHostStripe, stripeManager, field);
			final URI aIndexHostUri = chooseIndexHostUri(aHostStripe, stripeManager, field);
			logger.info("Syncing " + aIndexHostUri + " to " + aIndexHostMirrorDir);
			RemoteData rd = new RemoteData(aIndexHostUri.getHost());
			try {
				if (!aIndexHostMirrorDir.isDirectory() && !aIndexHostMirrorDir.mkdirs()) {
					logger.error("Directory " + aIndexHostMirrorDir + " cannot be created/accessed.");
					return false;
				}
				FileUtils.cleanDirectory(aIndexHostMirrorDir);
				rd.rsyncPull(aIndexHostMirrorDir, aIndexHostUri.getPath());
				return true;
			}
			finally {
				rd.close();
			}
		} // for buddies
		return false;
	}

	/**
	 * To be called only by leader of buddy set.  Note that this method will
	 * <b>always</b> wipe out the remote target directory and redo the push,
	 * which may take a long time! It will also <b>not</b> check the local
	 * directory to see if indexing is complete or the files are consistent.
	 * {@link RemoteData#rsyncPush(File, String)}s disk directory to buddies
	 * in preparation for {@link TokenCountsMerger} invocation.
	 */
	protected boolean syncPushToBuddies(AStripeManager stripeManager, Field field) throws Exception {
		if (stripeManager != null && stripeManager.myBuddyIndex() != 0) {
			logger.fatal("Should run this only on buddy master.");
			return false;
		}
		final URI myIndexDiskUri = chooseIndexDiskUri(stripeManager.myDiskStripe(), stripeManager, field);
		assert myIndexDiskUri.getHost().equals(stripeManager.myHostName());
		final File myIndexDiskDir = new File(myIndexDiskUri.getPath());
		final IntList buddies = stripeManager.buddyHostStripes(stripeManager.myDiskStripe());
		for (int aHostStripe : buddies) {
			if (aHostStripe == stripeManager.myHostStripe()) continue; // exclude myself
			final URI pushTarget = chooseIndexDiskRemoteDir(aHostStripe, stripeManager, field);
			logger.info("Pushing " + myIndexDiskUri + " to " + pushTarget);
			RemoteData rd = new RemoteData(pushTarget.getHost());
			try {
				if (rd.exists(pushTarget.getPath())) {
					rd.rmMinusRf(pushTarget.getPath());
				}
				rd.rsyncPush(myIndexDiskDir, pushTarget.getPath());
				return true;
			}
			finally {
				rd.close();
			}
		}
		return false;
	}

	/* The choosers based on field name */
	
	protected URI chooseIndexDiskRemoteDir(int aHostStripe, AStripeManager sm, Field af) throws URISyntaxException {
		switch (af) {
		case token:
			return sm.tokenIndexDiskRemoteDir(aHostStripe);
		case ent:
		case type:
			return sm.sipIndexDiskRemoteDir(aHostStripe);
		}
		throw new IllegalArgumentException(af.name());
	}
	
	protected URI chooseIndexDiskUri(int aDiskStripe, AStripeManager sm, Field af) throws URISyntaxException {
		switch (af) {
		case token:
			return sm.tokenIndexDiskDir(aDiskStripe);
		case type:
		case ent:
			return sm.sipIndexDiskDir(aDiskStripe);
		}
		throw new IllegalArgumentException(af.name());
	}
	
	protected File chooseIndexHostMirrorDir(int aHostStripe, AStripeManager sm, Field af) {
		switch (af) {
		case token:
			return sm.tokenIndexHostMirrorDir(aHostStripe);
		case type:
		case ent:
			return sm.sipIndexHostMirrorDir(aHostStripe);
		}
		throw new IllegalArgumentException(af.name());
	}
	
	protected URI chooseIndexHostUri(int aHostStripe, AStripeManager sm, Field af) throws URISyntaxException {
		switch (af) {
		case token:
			return sm.tokenIndexHostDir(aHostStripe);
		case type:
		case ent:
			return sm.sipIndexHostDir(aHostStripe);
		}
		throw new IllegalArgumentException(af.name());		
	}
}
