package iitb.CSAW.Spotter;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.util.Interval;

import java.io.Serializable;

public class Spot implements Comparable<Spot>, Serializable {
	private static final long serialVersionUID = 1L;
	
	public static final int naEnt = -1, unknownEnt = Integer.MIN_VALUE;
	
	public final int trieLeafNodeId;
	/**
	 * The "tight" mention span as matched in the trie. I.e., just the entity
	 * mention tokens, not any other tokens around them.
	 */
	public final Interval span;
	public final IntList entIds;
	
	public Spot(int trieLeafNodeId, Interval span, IntList entIds) {
		this.trieLeafNodeId = trieLeafNodeId;
		this.span = span;
		this.entIds = entIds;
	}
	
	@Override
	public String toString() {
		return getClass().getSimpleName() + "_L" + trieLeafNodeId + "@" + span + "E" + entIds.size() + entIds;
	}

	@Override
	public int compareTo(Spot o) {
		if (o.span.left > span.right) return -1;
		if (o.span.right < span.left) return 1;
		return span.left - o.span.left;
	}
}
