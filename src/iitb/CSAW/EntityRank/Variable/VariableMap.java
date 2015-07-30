package iitb.CSAW.EntityRank.Variable;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;;

@SuppressWarnings("serial")
public class VariableMap extends Object2IntOpenHashMap<BaseVariable> {
	public <V extends BaseVariable> void add(V var) {
		put(var, 1+size());
	}
	
	public int getIfExists(Object key) {
		if (containsKey(key)) {
			return getInt(key);
		}
		throw new IllegalArgumentException("key " + key + " is not mapped");
	}
}
