package iitb.CSAW.Index.SIP4;

import iitb.CSAW.Index.BaseIndexMerger;
import iitb.CSAW.Index.SipIndexMerger;
import iitb.CSAW.Utils.Config;

/**
 * SIP2 stub for using {@link SipIndexMerger}.
 */
public class Sip4IndexMerger extends BaseIndexMerger {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2..]=opcodes
	 * @throws Exception
	 * @see {@link SipIndexMerger}
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		SipIndexMerger<Sip4Document> ssim = new SipIndexMerger<Sip4Document>(config, Sip4Document.class);
		ssim.main(args);
	}
}
