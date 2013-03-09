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
    
    /** 
     * Returns file where the used owl:sameAs links should be written.
     * @return output file or null when the sameAs links shall not be written
     */
    File getSameAsFileLocation();
}
