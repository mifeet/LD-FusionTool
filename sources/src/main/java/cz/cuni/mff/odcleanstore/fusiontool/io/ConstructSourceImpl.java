/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.util.Map;

import org.openrdf.repository.Repository;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConstructSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;

/**
 * Container for RDF {@link Repository} and related settings.
 * @author Jan Michelfeit
 */
public final class ConstructSourceImpl extends SourceImpl implements ConstructSource {
    private String constructQuery;

    /**
     * Creates a new instance.
     * @param repository repository providing access to actual data
     * @param prefixes map of namespace prefixes
     * @param label name of this data source
     * @param type type of this data source
     * @param constructQuery SPARQL CONSTRUCT query generating input data
     */
    public ConstructSourceImpl(Repository repository, Map<String, String> prefixes,
            String label, EnumDataSourceType type, String constructQuery) {
        super(repository, prefixes, label, type);
        this.constructQuery = constructQuery;
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
    public static ConstructSource fromConfig(ConstructSourceConfig config, Map<String, String> prefixes,
            RepositoryFactory repositoryFactory) throws ODCSFusionToolException {
        Repository repository = repositoryFactory.createRepository(config);
        String label = config.getName() != null ? config.getName() : config.getType().toString();
        return new ConstructSourceImpl(
                repository,
                prefixes,
                label,
                config.getType(),
                config.getConstructQuery());
    }

    @Override
    public String getConstructQuery() {
        return constructQuery;
    }
}
