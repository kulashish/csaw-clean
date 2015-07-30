package iitb.CSAW.Utils;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.PropertyConfigurator;

/**
 * Sets up central properties repository as well as initialize logging.
 * Preferred over IR4QA and old CSAW Config classes because this supports
 * variables and includes.
 * @author soumen
 */
public class Config extends PropertiesConfiguration {
	public static final String nThreadsKey = "numThreads";

	public Config(String propName, String logName) throws ConfigurationException, IOException {
		Properties props = new Properties();
		FileReader pf = new FileReader(propName);
		props.load(pf);
		pf.close();
		props.setProperty("log4j.appender.R", "org.apache.log4j.FileAppender");
		props.setProperty("log4j.appender.R.File", logName);
		props.setProperty("log4j.appender.R.layout", "org.apache.log4j.SimpleLayout");
		PropertyConfigurator.configure(props);
		load(propName);
		setThrowExceptionOnMissing(true);
	}
}
