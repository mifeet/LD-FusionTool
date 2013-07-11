/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Type of an RDF Output.
 * @author Jan Michelfeit
 */
public enum EnumOutputType {
    /** Virtuoso JDBC connection. */
    VIRTUOSO,
    
    /** File. */
    FILE,
    
    /** SPARQL endpoint. */
    SPARQL
}
