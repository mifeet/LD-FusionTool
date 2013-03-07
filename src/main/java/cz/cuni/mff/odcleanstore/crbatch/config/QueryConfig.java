/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.util.Map;

/**
 * Settings concerning SPARQL queries.
 * @author Jan Michelfeit
 */
public interface QueryConfig {
    /**
     * SPARQL group graph pattern limiting source payload named graphs.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();

    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();
}
