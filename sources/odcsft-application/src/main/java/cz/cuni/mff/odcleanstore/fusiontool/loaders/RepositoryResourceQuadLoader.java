package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SparqlRestriction;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolQueryException;
import cz.cuni.mff.odcleanstore.fusiontool.source.DataSource;
import cz.cuni.mff.odcleanstore.shared.util.LimitedURIListBuilder;
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

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject)
 * from graphs matching the given named graph constraint pattern, taking into consideration
 * given owl:sameAs alternatives.
 * TODO: apply LIMIT/OFFSET
 */
public class RepositoryResourceQuadLoader extends RepositoryLoaderBase implements ResourceQuadLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RepositoryResourceQuadLoader.class);

    /**
     * SPARQL query that gets all quads having the given uri as their subject.
     * Quads are loaded from named graphs optionally limited by named graph restriction pattern.
     * This query is to be used when there are no owl:sameAs alternatives for the given URI.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) searched uri
     */
    private static final String QUADS_QUERY_SIMPLE = "%1$s" // TODO: distinct?
            + "\n SELECT DISTINCT (?%3$s AS ?" + VAR_PREFIX + "g)  (<%4$s> AS ?" + VAR_PREFIX + "s)" 
            + "\n   ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     <%4$s> ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n   }"
            + "\n }";

    /**
     * SPARQL query that gets all quads having one of the given URIs as their subject.
     * Quads are loaded from named graphs optionally limited by named graph restriction pattern.
     * This query is to be used when there are multiple owl:sameAs alternatives.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) list of searched URIs (e.g. "<uri1>,<uri2>,<uri3>")
     */
    private static final String QUADS_QUERY_ALTERNATIVE = "%1$s"
            + "\n SELECT DISTINCT (?%3$s AS ?" + VAR_PREFIX + "g)  ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "s ?" + VAR_PREFIX + "p ?" + VAR_PREFIX + "o"
            + "\n     FILTER (?" + VAR_PREFIX + "s IN (%4$s))" // TODO: replace by a large union for efficiency?
            + "\n   }"
            + "\n }";
    
    private static final String SUBJECT_VAR = VAR_PREFIX + "s";
    private static final String PROPERTY_VAR = VAR_PREFIX + "p";
    private static final String OBJECT_VAR = VAR_PREFIX + "o";
    private static final String GRAPH_VAR = VAR_PREFIX + "g";
    
    private final AlternativeUriNavigator alternativeUriNavigator;
    private RepositoryConnection connection;
    private final DataSource dataSource;

    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     * @param alternativeUriNavigator container of alternative owl:sameAs variants for URIs
     */
    public RepositoryResourceQuadLoader(DataSource dataSource, AlternativeUriNavigator alternativeUriNavigator) {
        super(dataSource);
        this.dataSource = dataSource;
        this.alternativeUriNavigator = alternativeUriNavigator;
    }

    /**
     * Adds quads having the given uri or one of its owl:sameAs alternatives as their subject to quadCollection.
     * Only quads from graph matching the data source's {@link DataSource#getNamedGraphRestriction() named graph restriction} will
     * be loaded.
     * @param uri searched subject URI
     * @param quadCollection collection to which the result will be added
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException error
     * @see DataSource#getNamedGraphRestriction()
     */
    @Override
    public void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws LDFusionToolException {
        long startTime = System.currentTimeMillis();
        uri = uri.trim(); // issue #2 fix
        
        SparqlRestriction restriction;
        if (dataSource.getNamedGraphRestriction() != null) {
            restriction = dataSource.getNamedGraphRestriction();
        } else {
            restriction = EMPTY_RESTRICTION;
        }
        
        List<String> alternativeURIs = alternativeUriNavigator.listAlternativeUris(uri);
        if (alternativeURIs.size() <= 1) {
            String query = formatQuery(QUADS_QUERY_SIMPLE, restriction, uri);
            try {
                addQuadsFromQuery(query, quadCollection);
            } catch (OpenRDFException e) {
                throw new LDFusionToolQueryException(LDFusionToolErrorCodes.QUERY_QUADS, query, source.getName(), e);
            }
        } else {
            Iterable<CharSequence> limitedURIListBuilder = new LimitedURIListBuilder(alternativeURIs, MAX_QUERY_LIST_LENGTH);
            for (CharSequence uriList : limitedURIListBuilder) {
                String query = formatQuery(QUADS_QUERY_ALTERNATIVE, restriction, uriList);
                try {
                    addQuadsFromQuery(query, quadCollection);
                } catch (OpenRDFException e) {
                    throw new LDFusionToolQueryException(LDFusionToolErrorCodes.QUERY_QUADS, query, source.getName(), e);
                }
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("ODCS-FusionTool: Loaded quads for URI {} from source {} in {} ms", new Object[] {
                    uri, source, System.currentTimeMillis() - startTime });
        }
    }
    
    private String formatQuery(String unformattedQuery, SparqlRestriction restriction, Object uriPart) {
        return String.format(Locale.ROOT, unformattedQuery,
                getPrefixDecl(),
                restriction.getPattern(),
                restriction.getVar(),
                uriPart);
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
        long startTime = System.currentTimeMillis();
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
                quads.add(quad);
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
    public void close() throws LDFusionToolException {
        try {
            closeConnection();
        } catch (RepositoryException e) {
            throw new LDFusionToolApplicationException(LDFusionToolErrorCodes.REPOSITORY_CLOSE, "Error closing repository connection");
        }
    }
}
