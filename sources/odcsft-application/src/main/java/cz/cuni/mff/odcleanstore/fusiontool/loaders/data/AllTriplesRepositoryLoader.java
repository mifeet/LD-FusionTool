package cz.cuni.mff.odcleanstore.fusiontool.loaders.data;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.RepositoryLoaderBase;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.fusiontool.util.LDFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.OutputParamReader;
import org.openrdf.OpenRDFException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.UUID;

import static cz.cuni.mff.odcleanstore.fusiontool.config.LDFTConfigConstants.*;

/**
 * Loader of all triples from  graphs matching the given named graph constraint pattern from an RDF repository.
 */
public class AllTriplesRepositoryLoader extends RepositoryLoaderBase implements AllTriplesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AllTriplesRepositoryLoader.class);
    public static final ValueFactoryImpl VALUE_FACTORY = ValueFactoryImpl.getInstance();

    private static final String SUBJECT_VAR = VAR_PREFIX + "s";
    private static final String PROPERTY_VAR = VAR_PREFIX + "p";
    private static final String OBJECT_VAR = VAR_PREFIX + "o";
    private static final String GRAPH_VAR = VAR_PREFIX + "g";

    /**
     * SPARQL query that gets all quads from named graphs optionally limited by named graph restriction pattern.
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) result size limit
     * (5) result offset
     */
    private static final String LOAD_SPARQL_QUERY = "%1$s"
            + "\n SELECT (?%3$s AS ?" + GRAPH_VAR + ")"
            + "\n   ?" + SUBJECT_VAR + " ?" + PROPERTY_VAR + " ?" + OBJECT_VAR
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + SUBJECT_VAR + " ?" + PROPERTY_VAR + " ?" + OBJECT_VAR
            + "\n   }"
            + "\n }"
            + "\n LIMIT %4$s OFFSET %5$s";
    private RepositoryConnection connection;

    private final DataSource dataSource;

    private final URI defaultContext;
    private final int maxSparqlResultsSize;
    private int initialOffset = 0;
    private int retryAttempts = 0;

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     */
    public AllTriplesRepositoryLoader(DataSource dataSource) {
        super(dataSource);
        this.dataSource = dataSource;
        OutputParamReader paramReader = new OutputParamReader(dataSource);
        this.defaultContext = computeDefaultContext(paramReader);
        this.maxSparqlResultsSize = paramReader.getIntValue(
                ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS,
                LDFTConfigConstants.DEFAULT_SPARQL_RESULT_MAX_ROWS);
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws LDFusionToolException {
        LOG.info("Parsing all quads from data source {}", source);
        String query = "";
        try {
            rdfHandler.startRDF();
            SparqlRestriction restriction = getSparqlRestriction();
            long totalStartTime = System.currentTimeMillis();
            int totalLoadedQuads = 0;
            int lastLoadedQuads = Integer.MAX_VALUE;
            for (int offset = initialOffset; lastLoadedQuads >= maxSparqlResultsSize; offset = initialOffset + totalLoadedQuads) {
                query = formatQuery(LOAD_SPARQL_QUERY, restriction, maxSparqlResultsSize, offset);
                long lastStartTime = System.currentTimeMillis();
                lastLoadedQuads = addQuadsFromQueryWithRetry(query, rdfHandler);
                totalLoadedQuads += lastLoadedQuads;
                logProgress(lastLoadedQuads, totalLoadedQuads, lastStartTime, totalStartTime);
            }
            rdfHandler.endRDF();
        } catch (OpenRDFException | InterruptedException e) {
            throw new LDFusionToolQueryException(LDFusionToolErrorCodes.ALL_TRIPLES_QUERY_QUADS, query, source.getName(), e);
        }
    }

    private void logProgress(int lastLoadedQuads, int totalLoadedQuads, long lastStartTime, long totalStartTime) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("ODCS-FusionTool: Loaded {} quads from source {} in {} ms",
                    new Object[] {lastLoadedQuads, source, System.currentTimeMillis() - lastStartTime});
        }
        if (totalLoadedQuads == lastLoadedQuads && lastLoadedQuads < maxSparqlResultsSize) {
            LOG.warn("Only one page of results with {} quads in total was loaded from query with limit {}."
                            + "\n       If you expect more data, the SPARQL endpoint may have a lower limit of rows returned from a SPARQL query."
                            + "\n       Try setting parameter {} to a value lower than {}.",
                    new Object[] {lastLoadedQuads, maxSparqlResultsSize, ConfigParameters.DATA_SOURCE_SPARQL_RESULT_MAX_ROWS, lastLoadedQuads});
        }
        if ((totalLoadedQuads - lastLoadedQuads) / LOG_LOOP_SIZE != totalLoadedQuads / LOG_LOOP_SIZE) {
            // show the log when the number of required quads was exceeded somewhere within the newly loaded quads
            LOG.info(String.format("ODCS-FusionTool: Loaded totally %,d quads from source %s so far in %s\n",
                    totalLoadedQuads, source, LDFusionToolUtils.formatProfilingTime(System.currentTimeMillis() - totalStartTime)));
        }
    }

    @Override
    public URI getDefaultContext() {
        return defaultContext;
    }

    private static URI computeDefaultContext(OutputParamReader paramReader) {
        String uri = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (uri != null && ODCSUtils.isValidIRI(uri)) {
            return VALUE_FACTORY.createURI(uri);
        }
        uri = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_SPARQL_ENDPOINT);
        if (uri != null && ODCSUtils.isValidIRI(uri)) {
            return VALUE_FACTORY.createURI(uri);
        }
        return VALUE_FACTORY.createURI("urn:uuid:", UUID.randomUUID().toString());
    }

    @Override
    public void close() throws LDFusionToolException {
        try {
            closeConnection();
        } catch (RepositoryException e) {
            LOG.error("Error closing repository connection", e);
        }
    }

    private String formatQuery(String unformattedQuery, SparqlRestriction restriction, int limit, int offset) {
        return String.format(Locale.ROOT,
                unformattedQuery,
                getPrefixDecl(),
                restriction.getPattern(),
                restriction.getVar(),
                limit,
                offset);
    }

    protected SparqlRestriction getSparqlRestriction() {
        SparqlRestriction restriction;
        if (dataSource.getNamedGraphRestriction() != null) {
            restriction = dataSource.getNamedGraphRestriction();
        } else {
            restriction = EMPTY_RESTRICTION;
        }
        return restriction;
    }


    private int addQuadsFromQueryWithRetry(String sparqlQuery, RDFHandler rdfHandler) throws OpenRDFException, InterruptedException {
        while (true) { // TODO: move constants to Configuration
            try {
                return addQuadsFromQuery(sparqlQuery, rdfHandler);
            } catch (OpenRDFException e) {
                retryAttempts++;
                if (retryAttempts <= REPOSITORY_RETRY_ATTEMPTS) {
                    String message = String.format("Query to repository %s failed, retry %d of %d in %d s",
                            source,
                            retryAttempts,
                            REPOSITORY_RETRY_ATTEMPTS,
                            REPOSITORY_RETRY_INTERVAL / ODCSUtils.MILLISECONDS);
                    LOG.warn(message, e);
                    Thread.sleep(REPOSITORY_RETRY_INTERVAL);
                } else {
                    String message = String.format("Query to repository %s failed, maximum number of retries of %d exceeded", source, REPOSITORY_RETRY_ATTEMPTS);
                    LOG.error(message, e);
                    throw e;
                }
            }
        }
    }

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * The query must contain four variables in the result, exactly in this order: named graph, subject,
     * property, object
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject,
     * property, object (exactly in this order).
     * @param rdfHandler handler to which retrieved quads are passed
     * @return number of retrieved quads
     */
    private int addQuadsFromQuery(String sparqlQuery, RDFHandler rdfHandler) throws OpenRDFException {
        int quadCount = 0;
        RepositoryConnection connection = getConnection();
        TupleQueryResult resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
        try {
            ValueFactory valueFactory = source.getRepository().getValueFactory();
            while (resultSet.hasNext()) {
                BindingSet bindings = resultSet.next();
                Statement quad = valueFactory.createStatement(
                        (Resource) bindings.getValue(SUBJECT_VAR),
                        (URI) bindings.getValue(PROPERTY_VAR),
                        bindings.getValue(OBJECT_VAR),
                        (Resource) bindings.getValue(GRAPH_VAR));
                rdfHandler.handleStatement(quad);
                quadCount++;
            }
        } finally {
            resultSet.close();
            if (source.getType() == EnumDataSourceType.VIRTUOSO) {
                // Issue #1 fix ("Too many open statements") - Virtuoso doesn't release resources properly
                try {
                    closeConnection();
                } catch (RepositoryException e) {
                    // ignore
                }
            }
        }
        return quadCount;
    }

    private RepositoryConnection getConnection() throws RepositoryException {
        if (connection == null) {
            connection = source.getRepository().getConnection();
        }
        return connection;
    }

    private void closeConnection() throws RepositoryException {
        if (connection != null) {
            try {
                connection.close();
            } finally {
                connection = null;
            }
        }
    }

    public void setInitialOffset(int initialOffset) {
        this.initialOffset = initialOffset;
    }
}
