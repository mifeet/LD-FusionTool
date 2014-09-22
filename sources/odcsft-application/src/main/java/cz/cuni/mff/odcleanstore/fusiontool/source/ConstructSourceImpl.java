/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.source;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConstructSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import org.openrdf.repository.Repository;

import java.util.Map;

/**
 * Container for RDF {@link Repository} and related settings.
 * @author Jan Michelfeit
 */
public final class ConstructSourceImpl extends SourceImpl implements ConstructSource {
    private final String constructQuery;

    /**
     * Creates a new instance.
     * @param repository repository providing access to actual data
     * @param prefixes map of namespace prefixes
     * @param label name of this data source
     * @param type type of this data source
     * @param params additional data source params
     * @param constructQuery SPARQL CONSTRUCT query generating input data
     */
    public ConstructSourceImpl(Repository repository, Map<String, String> prefixes,
            String label, EnumDataSourceType type, Map<String, String> params, String constructQuery) {
        super(repository, prefixes, label, type, params);
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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException invalid configuration
     */
    public static ConstructSource fromConfig(ConstructSourceConfig config, Map<String, String> prefixes,
            RepositoryFactory repositoryFactory) throws LDFusionToolException {
        Repository repository = repositoryFactory.createRepository(config);
        String label = config.getName() != null ? config.getName() : config.getType().toString();
        return new ConstructSourceImpl(
                repository,
                prefixes,
                label,
                config.getType(),
                config.getParams(),
                config.getConstructQuery());
    }

    @Override
    public String getConstructQuery() {
        return constructQuery;
    }
}
