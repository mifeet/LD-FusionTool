/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.util.HashMap;
import java.util.Map;

/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public class DataSourceConfigImpl implements DataSourceConfig {
    private final EnumDataSourceType type;
    private final String name;
    private final Map<String, String> params = new HashMap<String, String>();
    private SparqlRestriction namedGraphRestriction = 
            new SparqlRestrictionImpl("", ConfigConstants.DEFAULT_RESTRICTION_GRAPH_VAR);
    private SparqlRestriction metadataGraphRestriction;
    
    /**
     * @param type data source type
     * @param name human-readable name or null
     */
    public DataSourceConfigImpl(EnumDataSourceType type, String name) {
        this.type = type;
        this.name = name;
    }

    @Override
    public EnumDataSourceType getType() {
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
    public SparqlRestriction getNamedGraphRestriction() {
        return namedGraphRestriction;
    }
    
    /**
     * Setter for {@link #getNamedGraphRestriction()}.
     * @param restriction SPARQL group graph pattern
     */
    public void setNamedGraphRestriction(SparqlRestriction restriction) {
        if (restriction == null) {
            throw new IllegalArgumentException();
        } 
        this.namedGraphRestriction = restriction;
    }

    @Override
    public SparqlRestriction getMetadataGraphRestriction() {
        return metadataGraphRestriction;
    }
    
    /**
     * Setter for {@link #getMetadataGraphRestriction()}.
     * @param restriction SPARQL group graph pattern or null
     */
    public void setMetadataGraphRestriction(SparqlRestriction restriction) {
        this.metadataGraphRestriction = restriction;
    }
    
    @Override
    public String toString() {
        return name != null ? name : type.toString();
    }
}
