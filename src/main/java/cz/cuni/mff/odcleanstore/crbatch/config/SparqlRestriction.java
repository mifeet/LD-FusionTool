/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

/**
 * Class representing a restriction consisting of a SPARQL group graph pattern and the name of the restricted variable. 
 * @author Jan Michelfeit
 */
public interface SparqlRestriction {
    /**
     * Restricting SPARQL group graph pattern.
     * Value of {@link #getVar()} is the name of the SPARQL variable representing the restricted entity.
     * @return SPARQL group graph pattern
     */
    String getPattern();

    /**
     * Variable representing the restricted entity in {@link #getPattern()} value.
     * @return SPARQL variable name
     */
    String getVar();
}
