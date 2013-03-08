/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;

import cz.cuni.mff.odcleanstore.crbatch.io.EnumOutputFormat;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public interface Output {
    /**
     * Returns format (type) of the output.
     * @return output format
     */
    EnumOutputFormat getFormat();
    
    /**
     * Returns file where the output should be written.
     * @return output file
     */
    File getFileLocation();
}
