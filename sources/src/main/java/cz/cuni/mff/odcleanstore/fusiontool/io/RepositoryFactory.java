/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.File;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import virtuoso.sesame2.driver.VirtuosoRepository;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * Factory class for RDF {@link Repository repositories}.
 * @author Jan Michelfeit
 */
public final class RepositoryFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryFactory.class);

    /**
     * Creates a {@link Repository} based on the given configuration.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceConfig data source configuration
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createRepository(DataSourceConfig dataSourceConfig) throws ODCSFusionToolException {
        String name = dataSourceConfig.toString();
        switch (dataSourceConfig.getType()) {
        case VIRTUOSO:
            String host = dataSourceConfig.getParams().get("host");
            String port = dataSourceConfig.getParams().get("port");
            String username = dataSourceConfig.getParams().get("username");
            String password = dataSourceConfig.getParams().get("password");
            return createVirtuosoRepository(name, host, port, username, password);
        case SPARQL:
            String endpointUrl = dataSourceConfig.getParams().get("endpointurl");
            return createSparqlRepository(name, endpointUrl);
        case FILE:
            String path = dataSourceConfig.getParams().get("path");
            String format = dataSourceConfig.getParams().get("format");
            String baseURI = dataSourceConfig.getParams().get("baseuri");
            return createFileRepository(name, path, format, baseURI);
        default:
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_UNSUPPORTED, "Repository of type "
                    + dataSourceConfig.getType() + " is not supported");
        }
    }

    /**
     * Creates a repository from RDF data from a file.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceName data source name (for logging)
     * @param path path to the file to load data from
     * @param format serialization format (see {@link EnumSerializationFormat})
     * @param baseURI base URI
     * @return initialized repository
     * @throws ODCSFusionToolException error loading data from file
     */
    public Repository createFileRepository(String dataSourceName, String path, String format, String baseURI)
            throws ODCSFusionToolException {
        if (path == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameter for data source " + dataSourceName);
        }
        File file = new File(path);
        if (!file.isFile() || !file.canRead()) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_FILE, "Cannot read input file " + path);
        }
        if (baseURI == null) {
            baseURI = file.toURI().toString();
        }

        EnumSerializationFormat serializationFormat = EnumSerializationFormat.parseFormat(format);
        if (format != null && serializationFormat == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Unknown serialization format " + format + " for data source " + dataSourceName);
        }
        RDFFormat sesameFormat;
        if (serializationFormat == null) {
            sesameFormat = RDFFormat.forFileName(file.getName(), EnumSerializationFormat.RDF_XML.toSesameFormat());
        } else {
            sesameFormat = serializationFormat.toSesameFormat();
        }

        URI context = ValueFactoryImpl.getInstance().createURI(baseURI);
        Repository repository = new SailRepository(createSail());
        try {
            RepositoryConnection connection = null;
            repository.initialize();
            connection = repository.getConnection();
            try {
                connection.add(file, baseURI, sesameFormat, context);
            } finally {
                connection.close();
            }
        } catch (Exception e) {
            try {
                repository.shutDown();
            } catch (RepositoryException e2) {
                // ignore
            }
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_FILE,
                    "Cannot load data to repository for source " + dataSourceName, e);
        }
        LOG.debug("Initialized file repository {}", dataSourceName);
        return repository;
    }

    private Sail createSail() {
        MemoryStore store = new MemoryStore(); // TODO file-backed store if file caching is enabled?
        return store;
    }

    /**
     * Creates a repository backed by a Virtuoso JDBC connection.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceName data source name (for logging)
     * @param host host for the connection
     * @param port connection port
     * @param username connection username
     * @param password connection password
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createVirtuosoRepository(String dataSourceName, String host, String port,
            String username, String password) throws ODCSFusionToolException {
        if (host == null || port == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameters for data source " + dataSourceName);
        }
        String connectionString = "jdbc:virtuoso://" + host + ":" + port + "/CHARSET=UTF-8";
        Repository repository = new VirtuosoRepository(connectionString, username, password);
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_VIRTUOSO,
                    "Error when initializing repository for " + dataSourceName, e);
        }

        LOG.debug("Initialized Virtuoso repository {}", dataSourceName);
        return repository;
    }

    /**
     * Creates a repository backed by a SPARQL endpoint.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceName data source name (for logging)
     * @param endpointUrl SPARQL endpoint URL
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createSparqlRepository(String dataSourceName, String endpointUrl) throws ODCSFusionToolException {
        if (ODCSUtils.isNullOrEmpty(endpointUrl)) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameters for data source " + dataSourceName);
        }
        Repository repository = new SPARQLRepository(endpointUrl);
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_SPARQL,
                    "Error when initializing repository for " + dataSourceName, e);
        }
        LOG.debug("Initialized SPARQL repository {}", dataSourceName);
        return repository;
    }
    
    /**
     * Creates a repository backed by a SPARQL endpoint with update capabilities.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceName data source name (for logging)
     * @param endpointUrl SPARQL endpoint URL
     * @param updateEndpointUrl SPARQL update endpoint URL
     * @param username user name for the endpoint
     * @param password password for the endpoint
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createSparqlRepository(String dataSourceName, String endpointUrl, String updateEndpointUrl,
            String username, String password) throws ODCSFusionToolException {
        
        if (ODCSUtils.isNullOrEmpty(updateEndpointUrl) || ODCSUtils.isNullOrEmpty(endpointUrl)) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameters for data source " + dataSourceName);
        }
        SPARQLRepository repository = new SPARQLRepository(updateEndpointUrl, updateEndpointUrl);
        if (username != null || password != null) {
            repository.setUsernameAndPassword(username, password);
        }
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_SPARQL,
                    "Error when initializing repository for " + dataSourceName, e);
        }
        LOG.debug("Initialized SPARQL repository {}", dataSourceName);
        return repository;
    }
    
    /**
     * Creates a repository backed by a Virtuoso's SPARQL update endpoint.
     * Note that Virtuoso endpoint differs from the standards and therefore is incompatible with the 
     * standard SPARQL repository.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceName data source name (for logging)
     * @param endpointUrl SPARQL endpoint URL
     * @param updateEndpointUrl SPARQL update endpoint URL
     * @param username user name for the endpoint
     * @param password password for the endpoint
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createVirtuosoSparqlUpdateRepository(String dataSourceName, String endpointUrl, String updateEndpointUrl,
            String username, String password) throws ODCSFusionToolException {
        
        if (ODCSUtils.isNullOrEmpty(updateEndpointUrl) || ODCSUtils.isNullOrEmpty(endpointUrl)) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameters for data source " + dataSourceName);
        }
        SPARQLRepository repository = new VirtuosoSPARQLUpdateRepository(updateEndpointUrl, updateEndpointUrl);
        if (username != null || password != null) {
            repository.setUsernameAndPassword(username, password);
        }
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_VIRTUOSO_SPARQL,
                    "Error when initializing repository for " + dataSourceName, e);
        }
        LOG.debug("Initialized Virtuoso SPARQL repository {}", dataSourceName);
        return repository;
    }
}
