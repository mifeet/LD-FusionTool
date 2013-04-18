package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.crbatch.DataSource;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchQueryException;
import cz.cuni.mff.odcleanstore.crbatch.urimapping.AlternativeURINavigator;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;
import cz.cuni.mff.odcleanstore.shared.util.LimitedURIListBuilder;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject)
 * from graphs matching the given named graph constraint pattern, taking into consideration
 * given owl:sameAs alternatives.
 * @author Jan Michelfeit
 */
public class QuadLoader extends RepositoryLoaderBase implements Closeable {
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
     * (4) searched uri
     */
    private static final String QUADS_QUERY_SIMPLE = "%1$s"
            + "\n SELECT ?" + VAR_PREFIX + "g  (<%4$s> AS ?" + VAR_PREFIX + "s) ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   {"
            + "\n     SELECT DISTINCT "
            + "\n       (?%3$s AS ?" + VAR_PREFIX + "g)"
            + "\n       ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       GRAPH ?%3$s {"
            + "\n         <%4$s> ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n       }"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     SELECT DISTINCT"
            + "\n       (?" + VAR_PREFIX + "attachedGraph AS ?" + VAR_PREFIX + "g) ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n       GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n         <%4$s> ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
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
     * (4) list of searched URIs (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_ALTERNATIVE = "%1$s"
            + "\n SELECT ?" + VAR_PREFIX + "g ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   {"
            + "\n     SELECT DISTINCT "
            + "\n       (?%3$s AS ?" + VAR_PREFIX + "g)"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       GRAPH ?%3$s {"
            + "\n         ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n         FILTER (?" + VAR_PREFIX + "s IN (%4$s))"
            + "\n       }"
            + "\n     }"
            + "\n   }"
            + "\n   UNION"
            + "\n   {"
            + "\n     SELECT DISTINCT"
            + "\n       (?" + VAR_PREFIX + "attachedGraph AS ?" + VAR_PREFIX + "g)"
            + "\n       ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     WHERE {"
            + "\n       %2$s"
            + "\n       ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n       ?%3$s <" + ODCS.attachedGraph + "> ?" + VAR_PREFIX + "attachedGraph."
            + "\n       GRAPH ?" + VAR_PREFIX + "attachedGraph {"
            + "\n         ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n         FILTER (?" + VAR_PREFIX + "s IN (%4$s))"
            + "\n       }"
            + "\n     }"
            + "\n   }"
            + "\n }";

    private final AlternativeURINavigator alternativeURINavigator;
    private RepositoryConnection connection;

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     * @param alternativeURINavigator container of alternative owl:sameAs variants for URIs
     */
    public QuadLoader(DataSource dataSource, AlternativeURINavigator alternativeURINavigator) {
        super(dataSource);
        this.alternativeURINavigator = alternativeURINavigator;
    }

    /**
     * Adds quads having the given uri or one of its owl:sameAs alternatives as their subject to quadCollestion.
     * Only quads from graph matching the data source's {@link DataSource#getNamedGraphRestriction() named graph restriction} will
     * be loaded.
     * @param uri searched subject URI
     * @param quadCollection collection to which the result will be added
     * @throws CRBatchException error
     * @see DataSource#getNamedGraphRestriction()
     */
    public void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws CRBatchException {
        long startTime = System.currentTimeMillis();

        List<String> alternativeURIs = alternativeURINavigator.listAlternativeURIs(uri);
        if (alternativeURIs.size() <= 1) {
            String query = String.format(Locale.ROOT, QUADS_QUERY_SIMPLE,
                    getPrefixDecl(),
                    dataSource.getNamedGraphRestriction().getPattern(),
                    dataSource.getNamedGraphRestriction().getVar(),
                    uri);
            try {
                addQuadsFromQuery(query, quadCollection);
            } catch (OpenRDFException e) {
                throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_QUADS, query, dataSource.getName(), e);
            }
        } else {
            Iterable<CharSequence> limitedURIListBuilder = new LimitedURIListBuilder(alternativeURIs, MAX_QUERY_LIST_LENGTH);
            for (CharSequence uriList : limitedURIListBuilder) {
                String query = String.format(Locale.ROOT, QUADS_QUERY_ALTERNATIVE,
                        getPrefixDecl(),
                        dataSource.getNamedGraphRestriction().getPattern(),
                        dataSource.getNamedGraphRestriction().getVar(),
                        uriList);
                try {
                    addQuadsFromQuery(query, quadCollection);
                } catch (OpenRDFException e) {
                    throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_QUADS, query, dataSource.getName(), e);
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("CR-batch: Loaded quads for URI {} from source {} in {} ms", new Object[] {
                    uri, dataSource, System.currentTimeMillis() - startTime });
        }
    }

    /**
     * Execute the given SPARQL SELECT and constructs a collection of quads from the result.
     * The query must contain four variables in the result, exactly in this order: named graph, subject,
     * property, object
     * @param sparqlQuery a SPARQL SELECT query with four variables in the result: named graph, subject,
     *        property, object (exactly in this order).
     * @param quads collection where the retrieved quads are added
     * @throws OpenRDFException repository error
     */
    private void addQuadsFromQuery(String sparqlQuery, Collection<Statement> quads) throws OpenRDFException {
        final String subjectVar = VAR_PREFIX + "s";
        final String propertyVar = VAR_PREFIX + "p";
        final String objectVar = VAR_PREFIX + "o";
        final String graphVar = VAR_PREFIX + "g";

        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = getConnection();
        TupleQueryResult resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery).evaluate();
        try {
            LOG.trace("CR-batch: Quads query took {} ms", System.currentTimeMillis() - startTime);

            ValueFactory valueFactory = dataSource.getRepository().getValueFactory();
            while (resultSet.hasNext()) {
                BindingSet bindings = resultSet.next();
                Statement quad = valueFactory.createStatement(
                        (Resource) bindings.getValue(subjectVar),
                        (URI) bindings.getValue(propertyVar),
                        bindings.getValue(objectVar),
                        (Resource) bindings.getValue(graphVar));
                quads.add(quad);
            }
        } finally {
            resultSet.close();
        }
    }
    
    private RepositoryConnection getConnection() throws RepositoryException {
        if (connection == null) {
            connection = dataSource.getRepository().getConnection();
        }
        return connection;
    }
    
    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (RepositoryException e) {
                throw new IOException(e);
            } finally {
                connection = null;
            }
        }
    }
}
