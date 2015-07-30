package iitb.CSAW.Spotter;

import iitb.CSAW.Index.TokenCountsReader;
import iitb.CSAW.Utils.Config;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.configuration.ConfigurationException;

public class LogisticContextBase extends ContextBase {

	public LogisticContextBase(Config conf, TokenCountsReader tcr) throws IllegalArgumentException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException, IOException, ConfigurationException {
		super(conf, tcr);
	}

}
