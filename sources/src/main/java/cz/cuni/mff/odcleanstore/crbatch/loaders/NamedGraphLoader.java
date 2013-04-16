package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadata;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.crbatch.config.QueryConfig;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.shared.util.LimitedURIListBuilder;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Loads URIs and metadata of named graphs to be processed.
 * Only payload graphs are included.
 * @author Jan Michelfeit
 */
public class NamedGraphLoader extends RepositoryLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(NamedGraphLoader.class);
    
    /**
     * SPARQL query that gets metadata for named graphs to be processed.
     * Variable {@link #ngRestrictionVar}  represents the named graph.
     * Result contains variables {@link #ngRestrictionVar}, ?gp, ?go representing triples with
     * metadata about named graph ?{@link #ngRestrictionVar}.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * (4) graph name prefix filter
     * 
     * Note: Graphs without metadata are included too because at least odcs:metadataGraph value is expected.
     */
    private static final String METADATA_QUERY = "%1$s"
            + "\n SELECT DISTINCT ?%3$s ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   GRAPH ?%3$s {"
            + "\n     ?" + VAR_PREFIX + "x ?" + VAR_PREFIX + "y ?" + VAR_PREFIX + "z"
            + "\n   }"
            + "\n   ?%3$s <" + ODCS.metadataGraph + "> ?" + VAR_PREFIX + "metadataGraph."
            + "\n   ?%3$s ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go."
            + "\n   %4$s"
            + "\n }";
    
    /**
     * SPARQL query that gets the publisher scores for the given publishers.
     *
     * Must be formatted with arguments: (1) non-empty comma separated list of publisher URIs.
     */
    private static final String PUBLISHER_SCORE_QUERY = 
            "SELECT"
            + "\n   ?publishedBy ?score"
            + "\n WHERE {"
            + "\n   ?publishedBy <" + ODCS.publisherScore + "> ?score."
            + "\n   FILTER (?publishedBy IN (%1$s))"
            + "\n }";

    /**
     * Creates a new instance.
     * @param repository an initialized RDF repository
     * @param queryConfig Settings for SPARQL queries  
     */
    public NamedGraphLoader(Repository repository, QueryConfig queryConfig) {
        super(repository, queryConfig);
    }
    
    /**
     * Loads metadata for payload graphs matching the named graph constraint given in the constructor.
     * @return metadata for relevant named graphs
     * @throws CRBatchException error
     */
    public NamedGraphMetadataMap getNamedGraphs() throws CRBatchException {
        long startTime = System.currentTimeMillis();
        NamedGraphMetadataMap metadata = null;
        try {
            metadata = loadNamedGraphMetadata();
            addPublisherScores(metadata);
        } catch (OpenRDFException e) {
            throw new CRBatchException(CRBatchErrorCodes.QUERY_NG_METADATA, "Database error", e);
        }
        
        LOG.debug("CR-batch: Metadata loaded in {} ms", System.currentTimeMillis() - startTime);
        return metadata;
    }
    
    
    private NamedGraphMetadataMap loadNamedGraphMetadata() throws OpenRDFException {
        String query = String.format(Locale.ROOT, METADATA_QUERY,
                getPrefixDecl(),
                queryConfig.getNamedGraphRestriction().getPattern(),
                queryConfig.getNamedGraphRestriction().getVar(),
                getSourceNamedGraphPrefixFilter());
        final String graphVar = queryConfig.getNamedGraphRestriction().getVar();
        final String propertyVar = VAR_PREFIX + "gp";
        final String objectVar = VAR_PREFIX + "go";

        NamedGraphMetadataMap metadata = new NamedGraphMetadataMap();
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = getRepository().getConnection();
        TupleQueryResult resultSet = null;
        try {
            resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            LOG.debug("CR-batch: Metadata query took {} ms", System.currentTimeMillis() - startTime);
            
            // Build the result
            while (resultSet.hasNext()) {
                BindingSet bindings = resultSet.next();
                Resource namedGraphURI = (Resource) bindings.getValue(graphVar);
                NamedGraphMetadata graphMetadata = metadata.getMetadata(namedGraphURI);
                if (graphMetadata == null) {
                    graphMetadata = new NamedGraphMetadata(namedGraphURI.stringValue());
                    metadata.addMetadata(graphMetadata);
                }

                try {
                    String property = ((URI) bindings.getValue(propertyVar)).stringValue();

                    Value object = bindings.getValue(objectVar);
                    if (ODCS.source.equals(property)) {
                        graphMetadata.setSources(ODCSUtils.addToSetNullProof(object.stringValue(), graphMetadata.getSources()));
                    } else if (ODCS.score.equals(property) && object instanceof Literal) {
                        double score = ((Literal) object).doubleValue();
                        graphMetadata.setScore(score);
                    } else if (ODCS.insertedAt.equals(property) && object instanceof Literal) {
                        Date insertedAt = ((Literal) object).calendarValue().toGregorianCalendar().getTime();
                        graphMetadata.setInsertedAt(insertedAt);
                    } else if (ODCS.insertedBy.equals(property)) {
                        graphMetadata.setInsertedBy(object.stringValue());
                    } else if (ODCS.publishedBy.equals(property)) {
                        graphMetadata.setPublishers(ODCSUtils.addToListNullProof(
                                object.stringValue(), graphMetadata.getPublishers()));
                    } else if (ODCS.license.equals(property)) {
                        graphMetadata.setLicences(ODCSUtils.addToListNullProof(
                                object.stringValue(), graphMetadata.getLicences()));
                    } else if (ODCS.updateTag.equals(property)) {
                        graphMetadata.setUpdateTag(object.stringValue());
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("Invalid metadata for graph {}", namedGraphURI);
                }
            }
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            connection.close();
        }

        return metadata;
    }

    private void addPublisherScores(NamedGraphMetadataMap metadata) throws OpenRDFException {
        Map<String, Double> publisherScores = getPublisherScores(metadata);
        for (NamedGraphMetadata ngMetadata : metadata.listMetadata()) {
            Double publisherScore = calculatePublisherScore(ngMetadata, publisherScores);
            ngMetadata.setTotalPublishersScore(publisherScore);
        }
    }
    
    /**
     * Retrieve scores of publishers for all publishers occurring in given metadata.
     * @param metadata metadata retrieved for a query
     * @return map of publishers' scores
     * @throws OpenRDFException repositorys error
     */
    protected Map<String, Double> getPublisherScores(NamedGraphMetadataMap metadata)
            throws OpenRDFException {
        final String publisherVar = "publishedBy";
        final String scoreVar = "score";

        Map<String, Double> publisherScores = new HashMap<String, Double>();
        for (NamedGraphMetadata ngMetadata : metadata.listMetadata()) {
            List<String> publishers = ngMetadata.getPublishers();
            if (publishers != null) {
                for (String publisher : publishers) {
                    publisherScores.put(publisher, null);
                }
            }
        }

        Iterable<CharSequence> limitedURIListBuilder = new LimitedURIListBuilder(publisherScores.keySet(), MAX_QUERY_LIST_LENGTH);
        for (CharSequence publisherURIList : limitedURIListBuilder) {
            String query = String.format(Locale.ROOT, PUBLISHER_SCORE_QUERY, publisherURIList);
            long queryStartTime = System.currentTimeMillis();
            RepositoryConnection connection = getRepository().getConnection();
            TupleQueryResult resultSet = null;
            try {
                resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
                LOG.debug("CR-batch: getPublisherScores() query took {} ms", System.currentTimeMillis() - queryStartTime);

                while (resultSet.hasNext()) {
                    BindingSet bindings = resultSet.next();
                    
                    String publisher = "";
                    try {
                        publisher = ((URI) bindings.getValue(publisherVar)).stringValue();
                        Double score = ((Literal) bindings.getValue(scoreVar)).doubleValue();
                        publisherScores.put(publisher, score);
                    } catch (IllegalArgumentException e) {
                        LOG.warn("Query Execution: invalid publisher score for {}", publisher);
                    } catch (ClassCastException e) {
                        LOG.warn("Query Execution: invalid publisher score for {}", publisher);
                    }
                }
            } finally {
                if (resultSet != null) {
                    resultSet.close();
                }
                connection.close();
            }
        }

        return publisherScores;
    }

    /**
     * Calculates effective average publisher score - returns average of publisher scores or
     * null if there is none.
     * @param metadata named graph metadata; must not be null
     * @param publisherScores map of publisher scores
     * @return effective publisher score or null if unknown
     */
    protected Double calculatePublisherScore(final NamedGraphMetadata metadata, final Map<String, Double> publisherScores) {
        List<String> publishers = metadata.getPublishers();
        if (publishers == null) {
            return null;
        }
        double result = 0;
        int count = 0;
        for (String publisher : publishers) {
            Double score = publisherScores.get(publisher);
            if (score != null) {
                result += score;
                count++;
            }
        }
        return (count > 0) ? result / count : null;
    }

    
//    /**
//     * Extract named graph metadata from the result of the given SPARQL SELECT query.
//     * The query must contain three variables in the result, exactly in this order: named graph, property, value
//     * @param sparqlQuery a SPARQL SELECT query with three variables in the result: resGraph, property, value
//     * @param debugName named of the query used for debug log
//     * @return map of named graph metadata
//     * @throws DatabaseException database error
//     */

}
