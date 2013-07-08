/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.io.File;

import org.openrdf.model.URI;

import cz.cuni.mff.odcleanstore.fusiontool.io.EnumSerializationFormat;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public class OutputImpl implements Output {
    private final EnumSerializationFormat format;
    private final File fileLocation;
    private File sameAsFileLocation = null;
    private Long splitByBytes = null;
    private URI metadataContext = null;
    
    /**
     * @param format output format
     * @param fileLocation output file location
     */
    public OutputImpl(EnumSerializationFormat format, File fileLocation) {
        this.format = format;
        this.fileLocation = fileLocation;
    }
    
    @Override
    public EnumSerializationFormat getFormat() {
        return format;
    }

    @Override
    public File getFileLocation() {
        return fileLocation;
    }
    
    @Override
    public File getSameAsFileLocation() {
        return sameAsFileLocation;
    }
    
    /**
     * Sets value for {@link #getSameAsFileLocation()}.
     * @param sameAsFileLocation output file for owl:sameAs links
     */
    public void setSameAsFileLocation(File sameAsFileLocation) {
        this.sameAsFileLocation = sameAsFileLocation;
    }

    @Override
    public Long getSplitByBytes() {
        return splitByBytes; 
    }
    
    /**
     * Sets value for {@link #getSplitByBytes()}.
     * @param splitByBytes maximum size of one output file in bytes
     */
    public void setSplitByBytes(Long splitByBytes) {
        this.splitByBytes = splitByBytes;
    }
    
    @Override
    public URI getMetadataContext() {
        return metadataContext; 
    }

    /**
     * Sets value for {@link #getMetadataContext()}.
     * @param metadataContext see {@link #getMetadataContext()}
     */
    public void setMetadataContext(URI metadataContext) {
        this.metadataContext = metadataContext;
    }
}
