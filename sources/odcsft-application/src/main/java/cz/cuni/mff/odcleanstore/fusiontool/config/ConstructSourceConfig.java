/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;


/**
 * Settings concerning data inputs.
 * @author Jan Michelfeit
 */
public interface ConstructSourceConfig extends SourceConfig {
    /**
     * Returns the CONSTRUCT query which generates input triples from this source. Must not be null.
     * @return CONSTRUCT query
     */
    String getConstructQuery();
}
