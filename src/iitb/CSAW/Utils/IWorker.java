package iitb.CSAW.Utils;

import java.util.concurrent.Callable;

public interface IWorker extends Callable<Exception> {
	public long numDone();
}
