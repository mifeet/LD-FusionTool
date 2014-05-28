package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolApplicationUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.OutputParamReader;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import virtuoso.sesame2.driver.VirtuosoRepository;

import java.io.File;

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
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error creating repository
     */
    public Repository createRepository(SourceConfig dataSourceConfig) throws ODCSFusionToolException {
        String name = dataSourceConfig.toString();
        OutputParamReader paramReader = new OutputParamReader(dataSourceConfig);
        switch (dataSourceConfig.getType()) {
            case VIRTUOSO:
                return createVirtuosoRepository(name, paramReader);
            case SPARQL:
                return createSparqlRepository(name, paramReader);
            case FILE:
                return createFileRepository(name, paramReader);
            default:
                throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_UNSUPPORTED, "Repository of type "
                        + dataSourceConfig.getType() + " is not supported");
        }
    }

    private Repository createFileRepository(String dataSourceName, OutputParamReader paramReader)
            throws ODCSFusionToolException {
        Repository repository = new SailRepository(createSail());
        try {
            repository.initialize();
            ValueFactory valueFactory = repository.getValueFactory();
            RepositoryConnection connection = repository.getConnection();
            try {
                connection.setParserConfig(parserConfig);

                String path = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH);
                String format = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_FORMAT);
                String baseURI = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
                File file = new File(path);
                if (baseURI == null || !ODCSUtils.isValidIRI(baseURI)) {
                    baseURI = file.toURI().toString();
                }

                RDFFormat sesameFormat = ODCSFusionToolApplicationUtils.getSesameSerializationFormat(format, file.getName());
                if (sesameFormat == null) {
                    throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                            "Unknown serialization format " + format + " for data source " + dataSourceName);
                }

                // Avoid placing any triples in the default graph - they are poorly accessible via SPARQL
                URI defaultContext = connection.getValueFactory().createURI(baseURI);
                DefaultContextRDFInserter inserter = new DefaultContextRDFInserter(connection, defaultContext);
                RDFLoader loader = new RDFLoader(parserConfig, valueFactory);
                loader.load(file, baseURI, sesameFormat, inserter);
            } finally {
                connection.close();
            }
        } catch (Exception e) {
            try {
                repository.shutDown();
            } catch (RepositoryException e2) {
                // ignore
            }
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_FILE,
                    "Cannot load data to repository for source " + dataSourceName, e);
        }
        LOG.debug("Initialized file repository {}", dataSourceName);
        return repository;
    }

    private Sail createSail() {
        return new MemoryStore(); // TODO file-backed store if file caching is enabled?
    }

    private Repository createVirtuosoRepository(String name, OutputParamReader paramReader) throws ODCSFusionToolException {
        String host = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_HOST);
        String port = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_PORT);
        String username = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_USERNAME);
        String password = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_VIRTUOSO_PASSWORD);

        String connectionString = ODCSFusionToolApplicationUtils.getVirtuosoConnectionString(host, port);
        Repository repository = new VirtuosoRepository(connectionString, username, password);
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_VIRTUOSO,
                    "Error when initializing repository for " + name, e);
        }

        LOG.debug("Initialized Virtuoso repository {}", name);
        return repository;
    }

    private Repository createSparqlRepository(String dataSourceName, OutputParamReader paramReader)
            throws ODCSFusionToolException {

        String endpointUrl = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT);
        long minQueryIntervalMs = paramReader.getLongValue(ConfigParameters.DATA_SOURCE_SPARQL_MIN_QUERY_INTERVAL, -1);

        Repository repository = new WellBehavedSPARQLRepository(endpointUrl, minQueryIntervalMs);
        try {
            repository.initialize();
        } catch (RepositoryException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_SPARQL,
                    "Error when initializing repository for " + dataSourceName, e);
        }
        LOG.debug("Initialized SPARQL repository {}", dataSourceName);
        return repository;
    }

}
