package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadata;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.connection.WrappedResultSet;
import cz.cuni.mff.odcleanstore.connection.exceptions.DatabaseException;
import cz.cuni.mff.odcleanstore.connection.exceptions.QueryException;
import cz.cuni.mff.odcleanstore.crbatch.ConnectionFactory;
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
public class NamedGraphLoader extends DatabaseLoaderBase {
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
    private static final String METADATA_QUERY = "SPARQL %1$s"
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
    private static final String PUBLISHER_SCORE_QUERY = "SPARQL"
            + "\n SELECT"
            + "\n   ?publishedBy ?score"
            + "\n WHERE {"
            + "\n   ?publishedBy <" + ODCS.publisherScore + "> ?score."
            + "\n   FILTER (?publishedBy IN (%1$s))"
            + "\n }";

    /**
     * Creates a new instance.
     * @param connectionFactory factory for database connection
     * @param queryConfig Settings for SPARQL queries  
     */
    public NamedGraphLoader(ConnectionFactory connectionFactory, QueryConfig queryConfig) {
        super(connectionFactory, queryConfig);
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
        } catch (DatabaseException e) {
            throw new CRBatchException(CRBatchErrorCodes.QUERY_NG_METADATA, "Database error", e);
        } finally {
            closeConnectionQuietly();
        }
        
        LOG.debug("CR-batch: Metadata loaded in {} ms", System.currentTimeMillis() - startTime);
        return metadata;
    }
    
    
    private NamedGraphMetadataMap loadNamedGraphMetadata() throws DatabaseException {
        String query = String.format(Locale.ROOT, METADATA_QUERY,
                getPrefixDecl(),
                queryConfig.getNamedGraphRestriction().getPattern(),
                queryConfig.getNamedGraphRestriction().getVar(),
                getGraphPrefixFilter());
        final int graphIndex = 1;
        final int propertyIndex = 2;
        final int valueIndex = 3;
        
        long startTime = System.currentTimeMillis();
        WrappedResultSet resultSet = getConnection().executeSelect(query);
        LOG.debug("CR-batch: Metadata query took {} ms", System.currentTimeMillis() - startTime);

        // Build the result
        NamedGraphMetadataMap metadata = new NamedGraphMetadataMap();
        try {
            while (resultSet.next()) {
                String namedGraphURI = resultSet.getString(graphIndex);
                NamedGraphMetadata graphMetadata = metadata.getMetadata(namedGraphURI);
                if (graphMetadata == null) {
                    graphMetadata = new NamedGraphMetadata(namedGraphURI);
                    metadata.addMetadata(graphMetadata);
                }

                try {
                    String property = resultSet.getString(propertyIndex);

                    if (ODCS.source.equals(property)) {
                        String object = resultSet.getString(valueIndex);
                        graphMetadata.setSources(ODCSUtils.addToSetNullProof(object, graphMetadata.getSources()));
                    } else if (ODCS.score.equals(property)) {
                        Double score = resultSet.getDouble(valueIndex);
                        graphMetadata.setScore(score);
                    } else if (ODCS.insertedAt.equals(property)) {
                        Date insertedAt = resultSet.getJavaDate(valueIndex);
                        graphMetadata.setInsertedAt(insertedAt);
                    } else if (ODCS.insertedBy.equals(property)) {
                        String insertedBy = resultSet.getString(valueIndex);
                        graphMetadata.setInsertedBy(insertedBy);
                    } else if (ODCS.publishedBy.equals(property)) {
                        String object = resultSet.getString(valueIndex);
                        graphMetadata.setPublishers(ODCSUtils.addToListNullProof(object, graphMetadata.getPublishers()));
                    } else if (ODCS.license.equals(property)) {
                        String object = resultSet.getString(valueIndex);
                        graphMetadata.setLicences(ODCSUtils.addToListNullProof(object, graphMetadata.getLicences()));
                    } else if (ODCS.updateTag.equals(property)) {
                        String updateTag = resultSet.getString(valueIndex);
                        graphMetadata.setUpdateTag(updateTag);
                    }
                } catch (SQLException e) {
                    LOG.warn("Invalid metadata for graph {}", namedGraphURI);
                }
            }
        } catch (SQLException e) {
            throw new QueryException(e);
        } finally {
            resultSet.closeQuietly();
        }
        
        return metadata;
    }
        
    private void addPublisherScores(NamedGraphMetadataMap metadata) throws DatabaseException {
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
     * @throws DatabaseException database error
     */
    protected Map<String, Double> getPublisherScores(NamedGraphMetadataMap metadata)
            throws DatabaseException {
        final int publisherIndex = 1;
        final int scoreIndex = 2;

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
            WrappedResultSet resultSet = getConnection().executeSelect(query);
            LOG.debug("CR-batch: getPublisherScores() query took {} ms", System.currentTimeMillis() - queryStartTime);

            try {
                while (resultSet.next()) {
                    String publisher = "";
                    try {
                        publisher = resultSet.getString(publisherIndex);
                        Double score = resultSet.getDouble(scoreIndex);
                        publisherScores.put(publisher, score);
                    } catch (SQLException e) {
                        LOG.warn("Query Execution: invalid publisher score for {}", publisher);
                    }
                }
            } catch (SQLException e) {
                throw new QueryException(e);
            } finally {
                resultSet.closeQuietly();
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
