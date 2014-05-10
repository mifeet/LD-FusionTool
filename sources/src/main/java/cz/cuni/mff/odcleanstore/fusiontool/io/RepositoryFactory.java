package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ParamReader;
import org.openrdf.model.URI;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.nativerdf.NativeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import virtuoso.sesame2.driver.VirtuosoRepository;

import java.io.File;
import java.io.IOException;

/**
 * Factory class for RDF {@link Repository repositories}.
 * @author Jan Michelfeit
 */
public final class RepositoryFactory {
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryFactory.class);

    private final ParserConfig parserConfig;

    public RepositoryFactory(ParserConfig parserConfig) {
        ODCSFusionToolUtils.checkNotNull(parserConfig);
        this.parserConfig = parserConfig;
    }

    /**
     * Creates a {@link Repository} based on the given configuration.
     * The returned repository is initialized and the caller is responsible for calling {@link Repository#shutDown()}.
     * @param dataSourceConfig data source configuration
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createRepository(SourceConfig dataSourceConfig) throws ODCSFusionToolException {
        String name = dataSourceConfig.toString();
        ParamReader paramReader = new ParamReader(dataSourceConfig);
        switch (dataSourceConfig.getType()) {
            case VIRTUOSO:
                String host = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_HOST);
                String port = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_PORT);
                String username = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_USERNAME);
                String password = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_PASSWORD);
                return createVirtuosoRepository(name, host, port, username, password);
            case SPARQL:
                String endpointUrl = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT);
                long minQueryIntervalMs = paramReader.getLongValue(ConfigParameters.DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL, -1);
                return createSparqlRepository(name, endpointUrl, minQueryIntervalMs);
            case FILE:
                String path = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH);
                String format = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_FORMAT);
                String baseURI = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
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
        if (baseURI == null || ODCSUtils.isValidIRI(baseURI)) {
            baseURI = file.toURI().toString();
        }

        RDFFormat sesameFormat = ODCSFusionToolUtils.getSesameSerializationFormat(format, file.getName());
        if (sesameFormat == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Unknown serialization format " + format + " for data source " + dataSourceName);
        }

        Repository repository = new SailRepository(createSail());
        try {
            repository.initialize();
            loadFileToRepository(repository, file, sesameFormat, baseURI);
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

    private void loadFileToRepository(Repository repository, File file, RDFFormat sesameFormat, String baseURI)
            throws RepositoryException, IOException, RDFParseException, RDFHandlerException {

        RepositoryConnection connection = repository.getConnection();
        try {
            connection.setParserConfig(parserConfig);

            // Avoid placing any triples in the default graph - current implementation wouldn't see them
            URI defaultContext = connection.getValueFactory().createURI(baseURI);
            DefaultContextRDFInserter inserter = new DefaultContextRDFInserter(connection, defaultContext);
            RDFLoader loader = new RDFLoader(parserConfig, repository.getValueFactory());
            loader.load(file, baseURI, sesameFormat, inserter);
        } finally {
            connection.close();
        }
    }

    private Sail createSail() {
        return new NativeStore(); // TODO file-backed store if file caching is enabled?
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
        String connectionString = ODCSFusionToolUtils.getVirtuosoConnectionString(host, port);
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
     * @param minQueryIntervalMs minimum time in ms between SPARQL requests (-1 for no minimum time)
     * @return initialized repository
     * @throws ODCSFusionToolException error creating repository
     */
    public Repository createSparqlRepository(String dataSourceName, String endpointUrl, long minQueryIntervalMs)
            throws ODCSFusionToolException {

        if (ODCSUtils.isNullOrEmpty(endpointUrl)) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Missing required parameters for data source " + dataSourceName);
        }
        Repository repository = new WellBehavedSPARQLRepository(endpointUrl, minQueryIntervalMs);
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_SPARQL,
                    "Error when initializing repository for " + dataSourceName, e);
        }
        LOG.debug("Initialized SPARQL repository {}", dataSourceName);
        return repository;
    }

}
