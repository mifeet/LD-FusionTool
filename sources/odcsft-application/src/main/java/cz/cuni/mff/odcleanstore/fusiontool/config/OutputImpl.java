/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.URI;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public class OutputImpl implements Output {
    private final EnumOutputType type;
    private final String name;
    private final Map<String, String> params = new HashMap<String, String>();
    private URI metadataContext;
    private URI dataContext;
    
    /**
     * @param type output type
     * @param name human-readable name or null
     */
    public OutputImpl(EnumOutputType type, String name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public EnumOutputType getType() {
        return type;
    }
    
    @Override
    public String getName() {
        return name; 
    }
    
    @Override
    public Map<String, String> getParams() {
        return params;
    }

    @Override
    public URI getMetadataContext() {
        return metadataContext;
    }
    
    /**
     * Sets value for {@link #getMetadataContext()}.
     * @param metadataContext named graph URI
     */
    public void setMetadataContext(URI metadataContext) {
        this.metadataContext = metadataContext;
    }
    
    @Override
    public URI getDataContext() {
        return dataContext;
    }
    
    /**
     * Sets value for {@link #getDataContext()}.
     * @param dataContext named graph URI
     */
    public void setDataContext(URI dataContext) {
        this.dataContext = dataContext;
    }
    
    @Override
    public String toString() {
        return name != null ? name : type.toString();
    }
}
