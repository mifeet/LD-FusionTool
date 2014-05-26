/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Type of an RDF data source.
 * @author Jan Michelfeit
 */
public enum EnumDataSourceType {
    /** Virtuoso JDBC connection. */
    VIRTUOSO,
    
    /** File. */
    FILE,
    
    /** SPARQL endpoint. */
    SPARQL
}
