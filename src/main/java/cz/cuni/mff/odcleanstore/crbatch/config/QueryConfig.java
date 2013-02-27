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
     * Value of {@link #getNamedGraphRestrictionVar()} is the name of the SPARQL variable representing the payload graph.
     * @return SPARQL variable name
     */
    String getNamedGraphRestrictionPattern();

    /**
     * Variable representing named graphs in source named graph restriction pattern.
     * @return SPARQL variable name
     */
    String getNamedGraphRestrictionVar();

    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();
}
