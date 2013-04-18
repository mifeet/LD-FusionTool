/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;

import cz.cuni.mff.odcleanstore.crbatch.io.EnumSerializationFormat;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public interface Output {
    /**
     * Returns format (type) of the output.
     * @return output format
     */
    EnumSerializationFormat getFormat();

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

    /**
     * Returns the maximum size of output files in bytes. Larger files should by split into multiple files.
     * @return maximum size of one output file in bytes; null means do not split the file
     */
    Long getSplitByBytes();
}
