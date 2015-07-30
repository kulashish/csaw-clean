package iitb.CSAW.Index.SIP2;

import iitb.CSAW.Index.BaseIndexMerger;
import iitb.CSAW.Index.SipIndexMerger;
import iitb.CSAW.Utils.Config;

/**
 * SIP2 stub for using {@link SipIndexMerger}.
 */
public class Sip2IndexMerger extends BaseIndexMerger {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2..]=opcodes
	 * @throws Exception
	 * @see {@link SipIndexMerger}
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		SipIndexMerger<Sip2Document> ssim = new SipIndexMerger<Sip2Document>(config, Sip2Document.class);
		ssim.main(args);
	}
}
