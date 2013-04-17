/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch;

import java.util.Map;

import org.openrdf.repository.Repository;

import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestriction;

/**
 * Settings for an RDF data source.
 * @author Jan Michelfeit
 */
public interface DataSource {
    /**
     * Returns repository representing this data source.
     * @return repository representing the data source
     */
    Repository getRepository();

    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();

    /**
     * SPARQL group graph pattern limiting source payload named graphs. Must not be null.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();

    /**
     * SPARQL restriction on metadata named graphs.
     * @return SPARQL restriction (group graph pattern) or null
     */
    SparqlRestriction getMetadataGraphRestriction();
    
    /**
     * Returns a human-readable label.
     * @return label
     */
    String getName();
}
