package iitb.CSAW.EntityRank.Variable;

/**
 * Variable with just a name, not indexed by anything.
 * @author soumen
 */
public class BaseVariable {
	final String varBase;

	public BaseVariable(String varBase) {
		this.varBase = varBase;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BaseVariable)) return false;
		BaseVariable other = (BaseVariable) obj;
		return varBase.equals(other.varBase);
	}

	@Override
	public int hashCode() {
		return varBase.hashCode();
	}

	@Override
	public String toString() {
		return varBase.toString();
	}
}
