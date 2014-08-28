/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;


/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public class DataSourceConfigImpl extends SourceConfigImpl implements DataSourceConfig {
    private SparqlRestriction namedGraphRestriction = 
            new SparqlRestrictionImpl("", LDFTConfigConstants.DEFAULT_RESTRICTION_GRAPH_VAR);
    
    /**
     * @param type data source type
     * @param name human-readable name or null
     */
    public DataSourceConfigImpl(EnumDataSourceType type, String name) {
        super(type, name);
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
}
