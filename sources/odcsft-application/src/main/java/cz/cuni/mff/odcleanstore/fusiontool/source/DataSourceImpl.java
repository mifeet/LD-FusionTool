/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.source;

import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.RepositoryFactory;
import org.openrdf.repository.Repository;

import java.util.Map;

/**
 * Container for RDF {@link Repository} and related settings.
 * @author Jan Michelfeit
 */
public final class DataSourceImpl extends SourceImpl implements DataSource {
    private final SparqlRestriction namedGraphRestriction;

    /**
     * Creates a new instance.
     * @param repository repository providing access to actual data
     * @param prefixes map of namespace prefixes
     * @param label name of this data source
     * @param type type of this data source
     * @param params additional data source params
     * @param namedGraphRestriction SPARQL restriction on named graphs from which data are loaded
     */
    public DataSourceImpl(Repository repository, Map<String, String> prefixes,
            String label, EnumDataSourceType type, Map<String, String> params, SparqlRestriction namedGraphRestriction) {
        super(repository, prefixes, label, type, params);
        this.namedGraphRestriction = namedGraphRestriction;
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
                label,
                config.getType(),
                config.getParams(),
                config.getNamedGraphRestriction());
    }
    

    @Override
    public SparqlRestriction getNamedGraphRestriction() {
        return namedGraphRestriction;
    }
}
