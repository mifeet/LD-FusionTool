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
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadata;
import cz.cuni.mff.odcleanstore.conflictresolution.NamedGraphMetadataMap;
import cz.cuni.mff.odcleanstore.crbatch.DataSource;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchErrorCodes;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchQueryException;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Loads URIs and metadata of named graphs to be processed.
 * @author Jan Michelfeit
 */
public class NamedGraphMetadataLoader extends RepositoryLoaderBase {
    private static final Logger LOG = LoggerFactory.getLogger(NamedGraphMetadataLoader.class);
    
    /**
     * SPARQL query that gets metadata about named graphs to be processed.
     * Result contains variables ?gs, ?gp, ?go representing metadata triples.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     */
    private static final String METADATA_QUERY_NO_RESTRICTION = "%1$s"
            + "\n SELECT DISTINCT ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n WHERE {"
            + "\n   ?" + VAR_PREFIX + "gs <" + ODCS.metadataGraph + "> ?m."
            + "\n   ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go."
            + "\n }";
    
    /**
     * SPARQL query that gets metadata about named graphs to be processed.
     * Variable {@link #ngRestrictionVar}  represents the named graph.
     * Result contains variables ?gs, ?gp, ?go representing triples with
     * metadata.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) named graph restriction pattern
     * (3) named graph restriction variable
     * 
     * Note: Graphs without metadata are included too because at least odcs:metadataGraph value is expected.
     */
    private static final String METADATA_QUERY_SOURCE_RESTRICTION = "%1$s"
            + "\n SELECT DISTINCT (?%3$s AS ?" + VAR_PREFIX + "gs) ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n WHERE {"
            + "\n   %2$s"
            + "\n   ?%3$s ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go."
            + "\n }";
    
    /**
     * SPARQL query that gets metadata from named graphs restricted by metadata graph pattern
     * Variable {@link #ngRestrictionVar}  represents the named graph.
     * Result contains variables {@link #ngRestrictionVar}, ?gp, ?go representing triples with
     * metadata about named graph ?{@link #ngRestrictionVar}.
     * 
     * Must be formatted with arguments:
     * (1) namespace prefixes declaration
     * (2) metadata named graph restriction pattern
     * (3) metadata named graph restriction variable
     * 
     * Note: Graphs without metadata are included too because at least odcs:metadataGraph value is expected.
     */
    private static final String METADATA_QUERY_METADATA_RESTRICTION = "%1$s"
            + "\n SELECT DISTINCT ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n WHERE {"
            + "\n   %2$s" 
            + "\n   GRAPH ?%3$s {" 
            + "\n     ?" + VAR_PREFIX + "gs ?" + VAR_PREFIX + "gp ?" + VAR_PREFIX + "go"
            + "\n   }"
            + "\n }";

    private static final String SUBJECT_VAR = VAR_PREFIX + "gs";
    private static final String PROPERTY_VAR = VAR_PREFIX + "gp";
    private static final String OBJECT_VAR = VAR_PREFIX + "go";
    
    /**
     * Creates a new instance.
     * @param dataSource an initialized data source
     */
    public NamedGraphMetadataLoader(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * Loads relevant metadata and adds them to the given metadata collection.
     * Metadata are loaded for named graphs containing triples to be processed and metadata graphs.
     * @param metadata named graph metadata where loaded metadata are added
     * @throws CRBatchException repository error
     * @see DataSource#getMetadataGraphRestriction()
     */
    public void loadNamedGraphsMetadata(NamedGraphMetadataMap metadata) throws CRBatchException {
        long startTime = System.currentTimeMillis();
        String query = "";
        try {
            if (dataSource.getMetadataGraphRestriction() != null
                    && !ODCSUtils.isNullOrEmpty(dataSource.getMetadataGraphRestriction().getPattern())) {
                query = String.format(Locale.ROOT, METADATA_QUERY_METADATA_RESTRICTION,
                        getPrefixDecl(),
                        dataSource.getMetadataGraphRestriction().getPattern(),
                        dataSource.getMetadataGraphRestriction().getVar());
            } else if (dataSource.getNamedGraphRestriction() != null
                    && !ODCSUtils.isNullOrEmpty(dataSource.getNamedGraphRestriction().getPattern())) {
                query = String.format(Locale.ROOT, METADATA_QUERY_SOURCE_RESTRICTION,
                        getPrefixDecl(),
                        dataSource.getNamedGraphRestriction().getPattern(),
                        dataSource.getNamedGraphRestriction().getVar());
            } else {
                query = String.format(Locale.ROOT, METADATA_QUERY_NO_RESTRICTION, getPrefixDecl());
            }
            
            loadMetadataInternal(metadata, query);
        } catch (OpenRDFException e) {
            throw new CRBatchQueryException(CRBatchErrorCodes.QUERY_NG_METADATA, query, dataSource.getName(), e);
        }
        
        LOG.debug("CR-batch: Metadata loaded from source {} in {} ms", 
                dataSource.getName(), System.currentTimeMillis() - startTime);
    }
    
    private void loadMetadataInternal(NamedGraphMetadataMap metadata, String query) throws OpenRDFException {
        long startTime = System.currentTimeMillis();
        RepositoryConnection connection = dataSource.getRepository().getConnection();
        TupleQueryResult resultSet = null;
        try {
            resultSet = connection.prepareTupleQuery(QueryLanguage.SPARQL, query).evaluate();
            LOG.debug("CR-batch: Metadata query took {} ms", System.currentTimeMillis() - startTime);
            Map<String, Double> publisherScores = new HashMap<String, Double>();
            extractMetadata(resultSet, metadata, publisherScores);
            combineWithPublisherScores(metadata, publisherScores);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            connection.close();
        }
    }

    private void extractMetadata(TupleQueryResult resultSet, NamedGraphMetadataMap metadata, Map<String, Double> publisherScores)
            throws QueryEvaluationException {
        
        while (resultSet.hasNext()) {
            BindingSet bindings = resultSet.next();
            Resource subjectResource = (Resource) bindings.getValue(SUBJECT_VAR);

            try {
                String property = ((URI) bindings.getValue(PROPERTY_VAR)).stringValue();

                Value object = bindings.getValue(OBJECT_VAR);
                if (ODCS.source.equals(property)) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    graphMetadata.setSources(ODCSUtils.addToSetNullProof(object.stringValue(), graphMetadata.getSources()));
                } else if (ODCS.score.equals(property) && object instanceof Literal) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    double score = ((Literal) object).doubleValue();
                    graphMetadata.setScore(score);
                } else if (ODCS.insertedAt.equals(property) && object instanceof Literal) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    Date insertedAt = ((Literal) object).calendarValue().toGregorianCalendar().getTime();
                    graphMetadata.setInsertedAt(insertedAt);
                } else if (ODCS.insertedBy.equals(property)) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    graphMetadata.setInsertedBy(object.stringValue());
                } else if (ODCS.publishedBy.equals(property)) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    graphMetadata.setPublishers(ODCSUtils.addToListNullProof(
                            object.stringValue(), graphMetadata.getPublishers()));
                } else if (ODCS.license.equals(property)) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    graphMetadata.setLicences(ODCSUtils.addToListNullProof(
                            object.stringValue(), graphMetadata.getLicences()));
                } else if (ODCS.updateTag.equals(property)) {
                    NamedGraphMetadata graphMetadata = getGraphMetadata(subjectResource, metadata);
                    graphMetadata.setUpdateTag(object.stringValue());
                } else if (ODCS.publisherScore.equals(property)) {
                    Double score = ((Literal) object).doubleValue();
                    publisherScores.put(subjectResource.stringValue(), score);
                }
            } catch (IllegalArgumentException e) {
                LOG.warn("Invalid metadata for graph {}", subjectResource);
            }
        }
    }
    
    private NamedGraphMetadata getGraphMetadata(Resource namedGraphURI, NamedGraphMetadataMap metadataMap) {
        NamedGraphMetadata graphMetadata = metadataMap.getMetadata(namedGraphURI);
        if (graphMetadata == null) {
            graphMetadata = new NamedGraphMetadata(namedGraphURI.stringValue());
            metadataMap.addMetadata(graphMetadata);
        }
        return graphMetadata;
    }

    private void combineWithPublisherScores(NamedGraphMetadataMap metadata, Map<String, Double> publisherScores) {
        for (NamedGraphMetadata ngMetadata : metadata.listMetadata()) {
            Double publisherScore = calculatePublisherScore(ngMetadata, publisherScores);
            ngMetadata.setTotalPublishersScore(publisherScore);
        }
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
}
