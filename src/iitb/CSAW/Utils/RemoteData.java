package iitb.CSAW.Utils;

import it.unimi.dsi.logging.ProgressLogger;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.SftpProgressMonitor;

/**
 * Synchronizes local and remote dirs, as a replacement for using a network 
 * file system. Not thread-safe; can do only one operation at a time.
 * @author soumen
 */
public class RemoteData {
	static final String hext = "sha-256", allName = "all." + hext;

	final Logger logger = Logger.getLogger(getClass());
	final String myHost = InetAddress.getLocalHost().getHostName();
	final byte[] buf = new byte[8192];
	final String host;
	final Session session;
	final ChannelSftp csftp;
	
	public RemoteData(String host) throws Exception {
		this(host, 22);
	}
	
	public RemoteData(String host, int port) throws Exception {
		this.host = host;
		final JSch jsch=new JSch();
		final String dotSsh = System.getProperty("user.home") + File.separator + ".ssh"; 
		jsch.setKnownHosts(dotSsh + File.separator + "/known_hosts");
		jsch.addIdentity(dotSsh + File.separator + "id_dsa");
		session=jsch.getSession(System.getProperty("user.name"), host, port);
		session.connect();
		session.setConfig("cipher.s2c", "arcfour");
		session.setConfig("cipher.c2s", "arcfour");
		session.rekey();
		csftp = (ChannelSftp) session.openChannel("sftp");
		csftp.connect();
	}
	
	/**
	 * Must call this or main may not be able to exit properly.
	 */
	public void close() {
		csftp.quit();
		session.disconnect();
	}
	
	public InputStream getRemoteFileInputStream(String rem) throws JSchException, SftpException {
		return csftp.get(rem);
	}

	public boolean exists(String rem) {
		try {
			SftpATTRS attrs = csftp.stat(rem);
			attrs.getSize();
			return true;
		}
		catch (SftpException sx) {
			return false;
		}
	}
	
	/**
	 * Not nearly rsync in all its glory, but a utility method to replicate a
	 * remote directory to a local path.
	 * @param loc local directory, will be created if needed, but not cleaned out.
	 * @param host remote host
	 * @param rem remote path, interpreted wrt base of ssh login
	 * @throws JSchException
	 * @throws SftpException
	 * @throws IOException
	 */
	public void rsyncPull(File loc, String rem) throws JSchException, SftpException, IOException {
		if (!loc.isDirectory() && !loc.mkdirs()) {
			return;
		}
		@SuppressWarnings("unchecked") Vector<Object> lsOut = csftp.ls(rem);
		for (Object lsElem : lsOut) {
			if(lsElem instanceof com.jcraft.jsch.ChannelSftp.LsEntry){
				LsEntry lsEntry = (LsEntry) lsElem;
				SftpATTRS attrs = lsEntry.getAttrs();
				final String fname = lsEntry.getFilename();
				if (attrs.isDir()) {
					if (!fname.equals(".") && !fname.equals("..")) {
						rsyncPull(new File(loc, fname), rem + File.separator + fname);
					}
				}
				else {
					logger.info("Copying " + new File(loc, fname));
					final FileOutputStream fos = new FileOutputStream(new File(loc, fname));
					csftp.get(rem + File.separator + fname, fos, new RdMonitor());
					fos.close();
				}
			}
		}
	}
	
	public void rmMinusRf(String rem) throws SftpException {
		@SuppressWarnings("unchecked") Vector<Object> lsOut = csftp.ls(rem);
		for (Object lsElem : lsOut) {
			if(lsElem instanceof com.jcraft.jsch.ChannelSftp.LsEntry){
				LsEntry lsEntry = (LsEntry) lsElem;
				SftpATTRS attrs = lsEntry.getAttrs();
				final String fname = lsEntry.getFilename();
				if (attrs.isDir()) {
					if (!fname.equals(".") && !fname.equals("..")) {
						rmMinusRf(rem + File.separator + fname);
					}
				}
				else {
					logger.info("Rm " + rem + File.separator + fname);
					csftp.rm(rem + File.separator + fname);
				}
			}
		}
		logger.info("Rmdir " + host + ":" + rem);
		csftp.rmdir(rem);
	}

	public void rsyncPush(File loc, String rem) throws SftpException {
		if (!loc.isDirectory()) {
			throw new IllegalArgumentException(loc + " is not a directory.");
		}
		csftp.mkdir(rem);
		for (File sub : loc.listFiles()) {
			final String name = sub.getName();
			if (name.equals(".") || name.equals("..")) continue;
			if (sub.isDirectory()) {
				rsyncPush(sub, rem + File.separator + name);
			}
			else {
				logger.info("Copying " + sub);
				csftp.put(sub.getAbsolutePath(), rem + File.separator + name, new RdMonitor());
			}
		}
	}
	
	private class RdMonitor implements SftpProgressMonitor {
		long todo = 0, done = 0, start = System.currentTimeMillis(), last = start;
		@Override
		public void init(int op, String src, String dest, long max) {
			todo = max;
		}
		@Override
		public void end() {
		}
		@Override
		public boolean count(long count) {
			done += count;
			final long now = System.currentTimeMillis();
			if (now - last > ProgressLogger.ONE_MINUTE) {
				logger.info("\t\t" + String.format("%.0f", 100d * done / todo) + "% in " + (now-start)/ProgressLogger.ONE_SECOND + "s");
				last = now;
			}
			return true;
		}
	}
	
	/**
	 * Java equivalent of 
	 * <tt>find dirOrFile -type f -exec sha256sum -b {} \; | tee shaPs</tt>
	 * Works on local filesystem only.
	 * @param dirOrFile
	 * @param prefix used to print hte sha file
	 * @param shaPs usually null at first invocation
	 * @throws NoSuchAlgorithmException 
	 * @throws IOException 
	 */
	public static void generateChecksum(File dirOrFile, String prefix, PrintStream shaPs) throws NoSuchAlgorithmException, IOException {
		if (dirOrFile.isDirectory()) {
			if (shaPs == null) {
				shaPs = new PrintStream(new File(dirOrFile, allName));
			}
			for (File subFile : dirOrFile.listFiles()) {
				final String subName = subFile.getName();
				if (subName.equals(".") || subName.equals("..")) continue;
				if (subName.equals(allName)) continue;
				final String subPrefix = prefix.isEmpty()? subName : prefix + File.separator + subName;
				generateChecksum(subFile, subPrefix, shaPs);
			}
		}
		else {
			// assume shaPs is not null
			final MessageDigest md = MessageDigest.getInstance(hext);
			final FileInputStream payFis = new FileInputStream(dirOrFile);
			final byte[] buf = new byte[8192];
			for (int read = payFis.read(buf); read > 0; read = payFis.read(buf)) {
				md.update(buf, 0, read);
			}
			payFis.close();
			final byte[] digest = md.digest();
			shaPs.println(String.valueOf(Hex.encodeHex(digest)) + " *" + prefix);
		}
	}
	
	/**
	 * @param shaUri {@link URI} for file containing checksums. 
	 * The dot extension of this file is directly used to seed
	 * a {@link MessageDigest}.  Is typically "sha" or "sha-256"
	 * (note the hyphen).
	 * @return true if all files listed in the checksum file 
	 * are consistent with the listed checksums.
	 */
	public boolean verifyChecksum(URI shaUri) throws JSchException, SftpException, IOException, URISyntaxException, NoSuchAlgorithmException {
		final String shaPath = shaUri.getPath();
		final int lastDotPos = shaPath.lastIndexOf('.');
		final String ext = shaPath.substring(lastDotPos+1);
		MessageDigest md = MessageDigest.getInstance(ext);

		// load up local or remote sha file
		final InputStream shaIs;
		if (shaUri.getHost() == null || shaUri.getHost().equals(myHost)) {
			shaIs = new FileInputStream(shaUri.getPath());
		}
		else {
			shaIs = getRemoteFileInputStream(shaUri.getPath());
		}
		final InputStreamLineIterator isli = new InputStreamLineIterator(shaIs);
		while (isli.hasNext()) {
			final String line = isli.next();
			final Pattern pattern = Pattern.compile("^(\\S+)\\s+\\*?(.*)$");
			final Matcher matcher = pattern.matcher(line);
			if (!matcher.matches()) continue;
			final File payloadFile = new File(new File(shaUri.getPath()).getParentFile(), matcher.group(2));
			final URI payloadUri = new URI(shaUri.getScheme(), shaUri.getHost(), payloadFile.getAbsolutePath(), null);
			final boolean ok = digestMatches(md, matcher.group(1), payloadUri);
			logger.info(ok + " " + matcher.group(1) + " [" + payloadUri + "]");
			if (!ok) {
				return false;
			}
		}
		shaIs.close();
		return true;
	}
	
	protected boolean digestMatches(MessageDigest md, String refHash, URI payloadUri) throws NoSuchAlgorithmException, IOException, JSchException, SftpException {
		final InputStream payloadIs;
		if (payloadUri.getHost() == null || payloadUri.getHost().equals(myHost)) {
			payloadIs = new FileInputStream(payloadUri.getPath());
		}
		else {
			payloadIs = getRemoteFileInputStream(payloadUri.getPath());
		}
		md.reset();
		try {
			for (int read = payloadIs.read(buf), total = 0; read > 0; read = payloadIs.read(buf)) {
				md.update(buf, 0, read);
				total += read;
			}
		}
		catch (EOFException eofx) { }
		final byte[] digest = md.digest();
		final boolean ok = refHash.equals(String.valueOf(Hex.encodeHex(digest)));
		payloadIs.close();
		return ok;
	}
	
	private static void testGenerate(File dir) throws Exception {
		RemoteData.generateChecksum(dir, "", null);
	}
	
	private static void testVerify(URI shaUri) throws Exception {
		RemoteData rd = new RemoteData(shaUri.getHost(), shaUri.getPort());
		try {
			rd.verifyChecksum(shaUri);
		}
		finally {
			rd.close();
		}
	}
	
	private static void testExists(String host, String rem) throws Exception {
		RemoteData rd = new RemoteData(host);
		try {
			System.out.println(rem + " exists " + rd.exists(rem));
		}
		finally {
			rd.close();
		}
	}
	
	private static void testPull(File loc, String host, String rem) throws Exception {
		RemoteData rd = new RemoteData(host);
		if (!loc.isDirectory() && !loc.mkdirs()) {
			return;
		}
		FileUtils.cleanDirectory(loc);
		try {
			rd.rsyncPull(loc, rem);
		}
		finally {
			rd.close();
		}
	}
	
	private static void testPush(File loc, String host, String rem) throws Exception {
		RemoteData rd = new RemoteData(host);
		try {
			if (rd.exists(rem)) {
				rd.rmMinusRf(rem);
			}
			rd.rsyncPush(loc, rem);
		}
		finally {
			rd.close();
		}
	}

	/**
	 * Test harness.
	 * @param args [0]=opcode [1..]=args (see comments below)
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args[0].equals("generate")) {
			testGenerate(new File(args[1]));
		}
		else if (args[0].equals("verify")) {
			// uriOfShaFile
			testVerify(new URI(args[1]));			
		}
		else if (args[0].equals("exists")) {
			// host remPath
			testExists(args[1], args[2]);			
		}
		else if (args[0].equals("push")) {
			// localDir host remPath
			final File loc = new File(args[1]);
			testPush(loc, args[2], args[3]);
		}
		else if (args[0].equals("pull")) {
			// localDir host remPath
			final File loc = new File(args[1]);
			testPull(loc, args[2], args[3]);
		}
	}
}
