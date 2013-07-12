/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Type of an RDF Output.
 * @author Jan Michelfeit
 */
public enum EnumOutputType {
    /** Virtuoso SPARQL Update endpoint. */
    VIRTUOSO,
    
    /** File. */
    FILE,
    
    /** SPARQL endpoint. */
    SPARQL
}
