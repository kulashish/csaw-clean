package iitb.CSAW.Index.SIP3;

import iitb.CSAW.Index.BaseIndexMerger;
import iitb.CSAW.Index.SipIndexMerger;
import iitb.CSAW.Utils.Config;

/**
 * SIP3 stub for using {@link SipIndexMerger}.
 */
public class Sip3IndexMerger extends BaseIndexMerger {
	/**
	 * @param args [0]=/path/to/config [1]=/path/to/log [2]=opcode [3..]=opargs
	 * @throws Exception
	 * @see {@link SipIndexMerger}
	 */
	public static void main(String[] args) throws Exception {
		final Config config = new Config(args[0], args[1]);
		SipIndexMerger<Sip3Document> ssim = new SipIndexMerger<Sip3Document>(config, Sip3Document.class);
		ssim.main(args);
	}
}
