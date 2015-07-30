package iitb.CSAW.Corpus.Wikipedia;

import iitb.CSAW.Corpus.AStripeManager;
import iitb.CSAW.Utils.Config;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

import org.apache.commons.lang.NotImplementedException;

/**
 * The Barcelona corpus is all in one stripe, so this class provides a
 * compatibility implementation of {@link AStripeManager}.
 * 
 * @author soumen
 */
public class BarcelonaStripeManager extends AStripeManager {
	final Config conf;
	final String myHostName;
	
	public BarcelonaStripeManager(Config conf) throws UnknownHostException {
		this.conf = conf;
		myHostName = InetAddress.getLocalHost().getHostName(); // not fqdn
		conf.setProperty(hostNameKey, myHostName);
	}

	@Override public IntList buddyHostStripes(int diskStripe) {
		final IntArrayList ans = new IntArrayList();
		ans.add(0);
		return ans;
	}

	@Override public int myBuddyIndex() { return 0; }

	@Override public boolean isMyJob(long docId) { return true; }

	@Override public int numDiskStripes() { return 1; }

	@Override public int numHostStripes() { return 1; }

	@Override public int myDiskStripe() { return 0; }

	@Override public int myHostStripe() { return 0; }

	@Override
	public URI corpusDir(int diskStripe) throws URISyntaxException {
		return new URI(scheme, myHostName, conf.getString(iitb.CSAW.Corpus.Wikipedia.BarcelonaCorpus.storeDirName), null);
	}

	@Override
	public URI sipIndexDiskDir(int diskStripe) throws URISyntaxException {
		return new URI(scheme, myHostName, conf.getString(iitb.CSAW.Index.PropertyKeys.sipIndexDiskPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + diskStripe, null);
	}

	@Override
	public URI sipIndexHostDir(int hostStripe) throws URISyntaxException  {
		return new URI(scheme, myHostName, conf.getString(iitb.CSAW.Index.PropertyKeys.sipIndexHostPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + myDiskStripe() + File.separator + hostStripe, null);
	}

	@Override
	public File mySipIndexRunDir() {
		return new File(conf.getString(iitb.CSAW.Index.PropertyKeys.sipIndexRunPattern).replaceAll("\\$\\{HostName\\}", myHostName));
	}

	@Override
	public URI tokenIndexDiskDir(int diskStripe) throws URISyntaxException {
		return new URI(scheme, myHostName, conf.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexDiskPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + diskStripe, null);
	}

	@Override
	public URI tokenIndexHostDir(int hostStripe) throws URISyntaxException {
		return new URI(scheme, myHostName, conf.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexHostPattern).replaceAll("\\$\\{HostName\\}", myHostName) + File.separator + myDiskStripe() + File.separator + hostStripe, null);
	}

	@Override
	public File myTokenIndexRunDir() {
		return new File(conf.getString(iitb.CSAW.Index.PropertyKeys.tokenIndexRunPattern).replaceAll("\\$\\{HostName\\}", myHostName));
	}

	@Override
	public int hostNameToStripe(String hostName) {
		return 0;
	}

	@Override
	public String hostStripeToName(int hostStripe) {
		return myHostName;
	}

	@Override
	public int hostToDiskStripe(int hostStripe) {
		return 0;
	}

	@Override
	public String myHostName() {
		return myHostName;
	}

	@Override
	public File getTmpDir(int hostStripe) {
		return new File(conf.getString(tmpDirKey).replaceAll("\\$\\{HostName\\}", hostStripeToName(hostStripe)));
	}

	@Override
	public File sipIndexHostMirrorDir(int hostStripe) {
		throw new NotImplementedException();
	}

	@Override
	public File tokenIndexHostMirrorDir(int hostStripe) {
		throw new NotImplementedException();
	}

	@Override
	public URI sipIndexDiskRemoteDir(int aHostStripe) {
		throw new NotImplementedException();
	}

	@Override
	public URI tokenIndexDiskRemoteDir(int aHostStripe) {
		throw new NotImplementedException();
	}
}
