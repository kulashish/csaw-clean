package iitb.CSAW.Index;

public class PropertyKeys {
	/* Property keys saved to disk stripe */
	public static final String globalMaxDocument1 = "GlobalMaxDocumentPlus1";
	public static final String globalNumOccurrences = "GlobalNumOccurrences";

	/** For stemming, case-folding, etc. */
	public static final String termProcessorName = "TermProcessor";
	
	/* Token index */

	public static final String tokenIndexRunPattern = "TokenIndexRunPattern";
	public static final String tokenIndexHostPattern = "TokenIndexHostPattern";
	public static final String tokenIndexDiskPattern = "TokenIndexDiskPattern";
	
	public static final String tokenGlobalCfExtension = ".gcf";
	public static final String tokenGlobalDfExtension = ".gdf";
	public static final String tokenBijExtension = ".bij";
	
	/* Span-oriented SIP index (needs different directory from token index) */
	
	public static final String sipIndexRunPattern = "SipIndexRunPattern";
	public static final String sipIndexHostPattern = "SipIndexHostPattern";
	public static final String sipIndexDiskPattern = "SipIndexDiskPattern";
	
	public static final String sipCompactShaExtension = ".sha";
	public static final String sipCompactPostingExtension = ".csip";
	public static final String sipCompactSeekExtension = ".seek";
	public static final String sipCompactLocalDfExtension = ".ldf";
	public static final String sipCompactLocalCfExtension = ".lcf";
	public static final String sipCompactGlobalDfExtension = ".gdf";
	public static final String sipCompactGlobalCfExtension = ".gcf";
	
	/* For old token-oriented SIP1 index, to be phased out */
	
	public static final String sipUncompressedPostingsExtension = ".lsip";
	public static final String sipCompactOffsetExtension = ".csio";
	public static final String sipCompactNumDocExtension = ".csin";
	public static final String hostStripeKeyName = "HostStripe";
	public static final String diskStripeKeyName = "DiskStripe";
}
