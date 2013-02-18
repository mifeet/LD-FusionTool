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
public class OutputImpl implements Output {

    private final EnumOutputFormat format;
    private final File fileLocation;
    
    /**
     * @param format output format
     * @param fileLocation output file location
     */
    public OutputImpl(EnumOutputFormat format, File fileLocation) {
        this.format = format;
        this.fileLocation = fileLocation;
    }
    
    @Override
    public EnumOutputFormat getFormat() {
        return format;
    }

    @Override
    public File getFileLocation() {
        return fileLocation;
    }
}
