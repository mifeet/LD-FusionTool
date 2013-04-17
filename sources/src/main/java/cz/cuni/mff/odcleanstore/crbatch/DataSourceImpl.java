/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch;

import java.util.Map;

import org.openrdf.repository.Repository;

import virtuoso.sesame2.driver.VirtuosoRepository;
import cz.cuni.mff.odcleanstore.crbatch.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.crbatch.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;

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
    
    private DataSourceImpl(Repository repository, Map<String, String> prefixes, 
            SparqlRestriction namedGraphRestriction, SparqlRestriction metadataGraphRestriction, String label) {
        this.repository = repository;
        this.prefixes = prefixes;
        this.namedGraphRestriction = namedGraphRestriction;
        this.metadataGraphRestriction = metadataGraphRestriction;
        this.label = label;
    }
    
    /**
     * Creates RDF {@link DataSource data source} for the given configuration.
     * The Repository for the data sources is not initialized (this should be handled by the caller, 
     * as well as {@link Repository#shutDown() shutdown}).
     * @param config data source configuration
     * @param prefixes namespace prefixes for use in queries (specially in restriction patterns)
     * @return RDF data source
     * @throws CRBatchException invalid configuration
     */
    public static DataSource fromConfig(DataSourceConfig config, Map<String, String> prefixes) throws CRBatchException {
        Repository repository = createRepository(config);
        String label = config.getName() != null ? config.getName() : config.getType().toString();
        return new DataSourceImpl(
                repository,
                prefixes,
                config.getNamedGraphRestriction(),
                config.getMetadataGraphRestriction(),
                label);
    }
    
    private static Repository createRepository(DataSourceConfig dataSourceConfig) throws CRBatchException {
        Repository repository;
        switch (dataSourceConfig.getType()) {
        case VIRTUOSO:
            String host = dataSourceConfig.getParams().get("host");
            String port = dataSourceConfig.getParams().get("port");
            String username = dataSourceConfig.getParams().get("username");
            String password = dataSourceConfig.getParams().get("password");
            if (host == null || port == null) {
                throw new CRBatchException(CRBatchErrorCodes.REPOSITORY_CONFIG,
                        "Missing required parameters for data source of type " + dataSourceConfig.getType());
            }
            String connectionString = "jdbc:virtuoso://" + host + ":" + port + "/CHARSET=UTF-8";
            repository = new VirtuosoRepository(connectionString, username, password);
            break;
        default:
            throw new CRBatchException(CRBatchErrorCodes.REPOSITORY_UNSUPPORTED, "Repository of type "
                    + dataSourceConfig.getType() + " is not supported");
        }
        return repository;
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
}
