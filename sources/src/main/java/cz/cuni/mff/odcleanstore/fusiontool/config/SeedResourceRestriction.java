/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Seed resource .restriction consisting of {@link cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction}
 * and transitivity option
 */
public interface SeedResourceRestriction extends SparqlRestriction {
    /**
     * Indicates whether to resolve resources transitively starting from seed resources of a data source.
     * @return true if resources should be processed transitively
     */
    boolean isTransitive();
}

