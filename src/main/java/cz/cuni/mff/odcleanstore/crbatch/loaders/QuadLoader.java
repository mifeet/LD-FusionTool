package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;
import cz.cuni.mff.odcleanstore.connection.exceptions.QueryException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.queryexecution.impl.QueryExecutionHelper;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject)
 * from payload graphs matching the given named graph constraint pattern, taking into consideration
 * given owl:sameAs alternatives.
 * @author Jan Michelfeit
 */
public class QuadLoader extends DatabaseLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(QuadLoader.class);

    /**
     * SPARQL query that gets all quads having the given uri as their subject from
     * from relevant payload graph and attached graphs.
     * Variable {@link #ngRestrictionVar} represents a relevant payload graph.
     * This query is to be used when there are no owl:sameAs alternatives for the given URI.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     * (5) searched uri
     */
    private static final String QUADS_QUERY_SIMPLE = "SPARQL %1$s"
            + "\n SELECT ?" + VAR_PREFIX + "g  <%5$s> AS ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   {"
            + "\n     SELECT DISTINCT "
            + "\n       ?%3$s AS ?" + VAR_PREFIX + "g"
            + "\n       ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       GRAPH ?%3$s {"
            + "\n         <%5$s> ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n       }"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       %4$s"
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     SELECT DISTINCT"
            + "\n       ?" + VAR_PREFIX + "attachedGraph AS ?" + VAR_PREFIX + "g ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n       %4$s"
            + "\n       GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n         <%5$s> ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n       }"
            + "\n     }"
            + "\n   }"
            + "\n }";

    /**
     * SPARQL query that gets all quads having one of the given URIs as their subject from
     * from relevant payload graph and attached graphs.
     * Variable {@link #ngRestrictionVar} represents a relevant payload graph.
     * This query is to be used when there are multiple owl:sameAs alternatives.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     * (5) list of searched URIs (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_ALTERNATIVE = "SPARQL %1$s"
            + "\n SELECT ?" + VAR_PREFIX + "g ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   {"
            + "\n     SELECT DISTINCT "
            + "\n       ?%3$s AS ?" + VAR_PREFIX + "g"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       %4$s"
            + "\n       GRAPH ?%3$s {"
            + "\n         ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n         FILTER (?" + VAR_PREFIX + "s IN (%5$s))"
            + "\n       }"
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     SELECT DISTINCT"
            + "\n       ?" + VAR_PREFIX + "attachedGraph AS ?" + VAR_PREFIX + "g"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n       %4$s"
            + "\n       GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n         ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n         FILTER (?" + VAR_PREFIX + "s IN (%5$s))"
            + "\n       }"
            + "\n     }"
            + "\n   }"
            + "\n }";

    private final AlternativeURINavigator alternativeURINavigator;

    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param queryConfig Settings for SPARQL queries
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     */
    public QuadLoader(ConnectionFactory connectionFactory, QueryConfig queryConfig,
            AlternativeURINavigator alternativeURINavigator) {
        super(connectionFactory, queryConfig);
        this.alternativeURINavigator = alternativeURINavigator;
    }

    /**
     * Returns quads having the given uri or one of its owl:sameAs alternatives as their subject.
     * Triples are loaded from payload graphs matching the given named graph constraint pattern
     * and from their attached graphs.
     * @param uri searched subject URI
     * @return collection of quads having uri as their subject
     * @throws CRBatchException error
     */
    public Collection<Quad> getQuadsForURI(String uri) throws CRBatchException {
        long startTime = System.currentTimeMillis();
        ArrayList<Quad> result = new ArrayList<Quad>();
        try {

            List<String> alternativeURIs = alternativeURINavigator.listAlternativeURIs(uri);
            if (alternativeURIs.size() <= 1) {
                String query = String.format(Locale.ROOT, QUADS_QUERY_SIMPLE,
                        getPrefixDecl(),
                        queryConfig.getNamedGraphRestrictionPattern(),
                        queryConfig.getNamedGraphRestrictionVar(),
                        getGraphPrefixFilter(),
                        uri);
                addQuadsFromQuery(query, result);
            } else {
                Iterable<CharSequence> limitedURIListBuilder = QueryExecutionHelper.getLimitedURIListBuilder(alternativeURIs,
                        MAX_QUERY_LIST_LENGTH);
                for (CharSequence uriList : limitedURIListBuilder) {
                    String query = String.format(Locale.ROOT, QUADS_QUERY_ALTERNATIVE,
                            getPrefixDecl(),
                            queryConfig.getNamedGraphRestrictionPattern(),
                            queryConfig.getNamedGraphRestrictionVar(),
                            getGraphPrefixFilter(),
                            uriList);
                    addQuadsFromQuery(query, result);
                }
            }

        } catch (DatabaseException e) {
            throw new CRBatchException(CRBatchErrorCodes.QUERY_QUADS, "Database error", e);
        } finally {
            closeConnectionQuietly();
        }

        LOG.trace("CR-batch: Loaded quads for URI {} in {} ms", uri, System.currentTimeMillis() - startTime);
        return result;
    }

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * The query must contain four variables in the result, exactly in this order: named graph, subject,
     * property, object
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject,
     *        property, object (exactly in this order).
     * @param quads collection where the retrieved quads are added
     * @throws DatabaseException database error
     */
    private void addQuadsFromQuery(String sparqlQuery, Collection<Quad> quads) throws DatabaseException {
        long startTime = System.currentTimeMillis();
        WrappedResultSet resultSet = getConnection().executeSelect(sparqlQuery);
        LOG.trace("CR-batch: Quads query took {} ms", System.currentTimeMillis() - startTime);
        try {
            while (resultSet.next()) {
                // CHECKSTYLE:OFF
                Quad quad = new Quad(
                        resultSet.getNode(1),
                        resultSet.getNode(2),
                        resultSet.getNode(3),
                        resultSet.getNode(4));
                quads.add(quad);
                // CHECKSTYLE:ON
            }
        } catch (SQLException e) {
            throw new QueryException(e);
        } finally {
            resultSet.closeQuietly();
        }
    }
}
