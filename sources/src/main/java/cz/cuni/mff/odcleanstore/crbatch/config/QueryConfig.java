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
     * SPARQL group graph pattern limiting source payload named graphs. Must not be null.
     * @return SPARQL group graph pattern
     */
    SparqlRestriction getNamedGraphRestriction();

    /**
     * Map of namespace prefixes that can be used (e.g. in SPARQL expressions or aggregation settings).
     * Key is the prefix, value the expanded URI.
     * @return map of namespace prefixes
     */
    Map<String, String> getPrefixes();
    
    
    /**
     * SPARQL restriction on ontology named graphs. 
     * @return SPARQL restriction (group graph pattern) or null  
     */
    SparqlRestriction getOntologyGraphRestriction();
    
    /**
     * SPARQL restriction on URI resources which are initially loaded and processed.
     * If given, triples having matching resources and triples reachable from them are processed. All data
     * from matching input graphs are processed otherwise.
     * @return SPARQL restriction (group graph pattern) or null  
     */
    SparqlRestriction getSeedResourceRestriction();
}
