package iitb.CSAW.Utils;

import org.apache.commons.lang.mutable.MutableLong;

import it.unimi.dsi.Util;

public class MemoryStatus {
	static Runtime RUNTIME = Runtime.getRuntime();
	MutableLong used = new MutableLong(), avail = new MutableLong(), free = new MutableLong(), total = new MutableLong(), max = new MutableLong();
	
	public static void get(MutableLong used, MutableLong avail, MutableLong free, MutableLong total, MutableLong max) {
		used.setValue( RUNTIME.totalMemory() - RUNTIME.freeMemory() );
		avail.setValue( RUNTIME.freeMemory() + ( RUNTIME.maxMemory() - RUNTIME.totalMemory()) );
		free.setValue( RUNTIME.freeMemory() );
		total.setValue( RUNTIME.totalMemory() );
		max.setValue( RUNTIME.maxMemory() );
	}
	
	public boolean isAvailableBelowThreshold(double fraction) {
		get(used, avail, free, total, max);
		return avail.doubleValue() < fraction * max.doubleValue();
	}
	
	@Override
	public String toString() {
		get(used, avail, free, total, max);
		return "used/avail/free/total/max mem: " 
				+ Util.formatSize( used.longValue() ) + "/" 
				+ Util.formatSize( avail.longValue() ) + "/" 
				+ Util.formatSize( free.longValue() ) + "/" 
				+ Util.formatSize( total.longValue() ) + "/" 
				+ Util.formatSize( max.longValue() );
	}
}
