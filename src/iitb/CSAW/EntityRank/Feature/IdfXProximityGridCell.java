package iitb.CSAW.EntityRank.Feature;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.NotImplementedException;

import iitb.CSAW.EntityRank.Wikipedia.Snippet;
import iitb.CSAW.Index.ContextWitness;
import iitb.CSAW.Query.RootQuery;
import iitb.CSAW.Utils.Config;
import cern.colt.list.DoubleArrayList;

public class IdfXProximityGridCell extends AFeature {
	
	int idfIndexLowToHigh;
	int proximityIndexNearToFar;
	double value = Double.MAX_VALUE; 
	
	public IdfXProximityGridCell(Config config, int idfIndexLowToHigh, int proximityIndexNearToFar) throws IOException, ClassNotFoundException, ConfigurationException, SecurityException, URISyntaxException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
		super(config);
		this.idfIndexLowToHigh = idfIndexLowToHigh;
		this.proximityIndexNearToFar = proximityIndexNearToFar;
	}

	@Override
	public double value(RootQuery query, Snippet snippet) {
		throw new NotImplementedException();
	}

	@Override
	public double value2(ContextWitness cw){
		return value;
	}

	@Override
	public double value3(ContextWitness cw, DoubleArrayList positions,
			DoubleArrayList values) {
		throw new NotImplementedException();
	}

}
