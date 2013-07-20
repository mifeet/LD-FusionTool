/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.util.Map;

import org.openrdf.repository.Repository;

import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;

/**
 * Container for RDF {@link Repository} and related settings.
 * @author Jan Michelfeit
 */
public final class DataSourceImpl implements DataSource {
    private final Repository repository;
    private final Map<String, String> prefixes;
    private final SparqlRestriction namedGraphRestriction;
    private final SparqlRestriction metadataGraphRestriction;
    private final String label;
    private final EnumDataSourceType type;

    /**
     * Creates a new instance.
     * @param repository repository providing access to actual data
     * @param prefixes map of namespace prefixes
     * @param namedGraphRestriction SPARQL restriction on named graphs from which data are loaded
     * @param metadataGraphRestriction SPARQl restriction on named graphs from which metadata are loaded
     * @param label name of this data source
     * @param type type of this data source
     */
    public DataSourceImpl(Repository repository, Map<String, String> prefixes,
            SparqlRestriction namedGraphRestriction, SparqlRestriction metadataGraphRestriction, String label,
            EnumDataSourceType type) {
        this.repository = repository;
        this.prefixes = prefixes;
        this.namedGraphRestriction = namedGraphRestriction;
        this.metadataGraphRestriction = metadataGraphRestriction;
        this.label = label;
        this.type = type;
    }

    /**
     * Creates RDF {@link DataSource data source} for the given configuration.
     * The Repository for the data sources is not initialized (this should be handled by the caller,
     * as well as {@link Repository#shutDown() shutdown}).
     * @param config data source configuration
     * @param prefixes namespace prefixes for use in queries (specially in restriction patterns)
     * @param repositoryFactory repository factory
     * @return RDF data source
     * @throws ODCSFusionToolException invalid configuration
     */
    public static DataSource fromConfig(DataSourceConfig config, Map<String, String> prefixes,
            RepositoryFactory repositoryFactory) throws ODCSFusionToolException {
        Repository repository = repositoryFactory.createRepository(config);
        String label = config.getName() != null ? config.getName() : config.getType().toString();
        return new DataSourceImpl(
                repository,
                prefixes,
                config.getNamedGraphRestriction(),
                config.getMetadataGraphRestriction(),
                label,
                config.getType());
    }

    @Override
    public Repository getRepository() {
        return repository;
    }

    @Override
    public Map<String, String> getPrefixes() {
        return prefixes;
    }

    @Override
    public SparqlRestriction getNamedGraphRestriction() {
        return namedGraphRestriction;
    }

    @Override
    public SparqlRestriction getMetadataGraphRestriction() {
        return metadataGraphRestriction;
    }

    @Override
    public String getName() {
        return label;
    }

    @Override
    public String toString() {
        return getName();
    }

    @Override
    public EnumDataSourceType getType() {
        return type;
    }
}
