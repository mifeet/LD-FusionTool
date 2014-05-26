/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;


/**
 * Container of settings for an output of result data.
 * @author Jan Michelfeit
 */
public class ConstructSourceConfigImpl extends SourceConfigImpl implements ConstructSourceConfig {
    private String constructQuery; 
    
    /**
     * @param type data source type
     * @param name human-readable name or null
     * @param constructQuery SPARQL CONSTRUCT query
     */
    public ConstructSourceConfigImpl(EnumDataSourceType type, String name, String constructQuery) {
        super(type, name);
        this.constructQuery = constructQuery;
    }

    @Override
    public String getConstructQuery() {
        return constructQuery;
    }
    
    /**
     * Setter for {@link #getConstructQuery()()}.
     * @param constructQuery SPARQL CONSTRUCT query
     */
    public void setConstructQuery(String constructQuery) {
        if (constructQuery == null) {
            throw new IllegalArgumentException();
        } 
        this.constructQuery = constructQuery;
    }
}
