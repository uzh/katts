package ch.uzh.ddis.katts.bolts.output;

import ch.uzh.ddis.katts.bolts.Configuration;

/**
 * This interface defines the configurations required by {@link FileOutputBolt}.
 * 
 * @author Thomas Hunziker
 *
 */
public interface FileOutputConfiguration extends Configuration{

	/**
	 * This method returns the path of the file, where the output data should be stored in.
	 * 
	 * @return The path to the output file.
	 */
	public String getFilePath();
	
	/** 
	 * If true, an exception will be thrown if any of the values of the incoming streams is <b>null</b>.
	 * 
	 * Default is <b>false</b>.
	 */
	public boolean isAllowNullValues();
	
}
