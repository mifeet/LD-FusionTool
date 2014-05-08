package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.io.DataSource;
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
    private final int maxSparqlResultsSize;

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     * @param maxSparqlResultsSize maximum number of triples to be returned in a single query (LIMIT clause)
     */
    public AllTriplesRepositoryLoader(DataSource dataSource, int maxSparqlResultsSize) {
        super(dataSource);
        this.dataSource = dataSource;
        this.maxSparqlResultsSize = maxSparqlResultsSize;
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws ODCSFusionToolException {
        LOG.info("Parsing all quads from data source {}", source);
        String query = "";
        try {
            rdfHandler.startRDF();
            SparqlRestriction restriction = getSparqlRestriction();
            int loadedQuads = Integer.MAX_VALUE;
            for (int offset = 0; loadedQuads >= maxSparqlResultsSize; offset += maxSparqlResultsSize) {
                query = formatQuery(LOAD_SPARQL_QUERY, restriction, maxSparqlResultsSize, offset);
                long startTime = System.currentTimeMillis();
                loadedQuads = addQuadsFromQuery(query, rdfHandler);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ODCS-FusionTool: Loaded {} quads from source {} in {} ms",
                            new Object[]{loadedQuads, source, System.currentTimeMillis() - startTime});
                }
            }
            rdfHandler.endRDF();
        } catch (OpenRDFException e) {
            throw new ODCSFusionToolQueryException(ODCSFusionToolErrorCodes.ALL_TRIPLES_QUERY_QUADS, query, source.getName(), e);
        }
    }

    @Override
    public void close() throws ODCSFusionToolException {
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

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * The query must contain four variables in the result, exactly in this order: named graph, subject,
     * property, object
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject,
     *        property, object (exactly in this order).
     * @param rdfHandler handler to which retrieved quads are passed
     * @return number of retrieved quads
     * @throws org.openrdf.OpenRDFException repository error
     */
    private int addQuadsFromQuery(String sparqlQuery, RDFHandler rdfHandler) throws OpenRDFException {
        long startTime = System.currentTimeMillis();
        int quadCount = 0;
        RepositoryConnection connection = getConnection();
        TupleQueryResult resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
        try {
            LOG.trace("ODCS-FusionTool: Quads query took {} ms", System.currentTimeMillis() - startTime);

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

    @Override
    public URI getDefaultContext() {
        String uri;
        if (ODCSUtils.isValidIRI(dataSource.getName())) {
            uri = dataSource.getName();
        } else {
            uri = "http://" + dataSource.getName().replaceAll("[<>\"{}|^`\\x00-\\x20']", "");
        }
        return VALUE_FACTORY.createURI(uri);
    }
}
