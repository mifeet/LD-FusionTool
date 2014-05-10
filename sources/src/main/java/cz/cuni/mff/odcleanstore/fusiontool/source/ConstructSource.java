/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.source;


/**
 * Settings for an RDF data source.
 * @author Jan Michelfeit
 */
public interface ConstructSource extends Source {
    /**
     * CONSTRUCT SPARQL query generating the input RDF data. Must not be null.
     * @return SPARQL CONSTRUCT query
     */
    String getConstructQuery();
}
