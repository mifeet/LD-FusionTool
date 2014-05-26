/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;


/**
 * Settings concerning data inputs.
 * @author Jan Michelfeit
 */
public interface DataSourceConfig extends SourceConfig {
    /**
     * SPARQL group graph pattern limiting source payload named graphs. Must not be null.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();
}
