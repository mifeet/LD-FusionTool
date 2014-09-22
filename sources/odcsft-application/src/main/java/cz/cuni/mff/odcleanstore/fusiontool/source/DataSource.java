/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.source;

import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;

/**
 * Settings for an RDF data source.
 * @author Jan Michelfeit
 */
public interface DataSource extends Source {
    /**
     * SPARQL group graph pattern limiting source payload named graphs. Must not be null.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();

}
