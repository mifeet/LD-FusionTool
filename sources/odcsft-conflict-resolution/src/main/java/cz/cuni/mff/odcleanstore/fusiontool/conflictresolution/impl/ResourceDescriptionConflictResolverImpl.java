package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.collect.Table;
import cz.cuni.mff.odcleanstore.conflictresolution.*;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ResolutionFunctionNotRegisteredException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.CRContextImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementFactoryImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.AllResolution;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.EmptyUriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.ClusterIterator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.ODCSFusionToolCRUtils;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.StatementMapper;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * TODO
 */
public class ResourceDescriptionConflictResolverImpl implements ResourceDescriptionConflictResolver {
    private static final Logger LOG = LoggerFactory.getLogger(ResourceDescriptionConflictResolverImpl.class);

    /** Default conflict resolution strategy. */
    public static final ResolutionStrategy DEFAULT_RESOLUTION_STRATEGY = new ResolutionStrategyImpl(
            AllResolution.getName(),
            EnumCardinality.MANYVALUED,
            EnumAggregationErrorStrategy.RETURN_ALL);

    /** Default prefix of graph names where resolved quads are placed. */
    public static final String DEFAULT_RESOLVED_GRAPHS_URI_PREFIX = ODCS.NAMESPACE + "cr/";

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();
    private static final SortedListModelFactory SORTED_LIST_MODEL_FACTORY = new SortedListModelFactory();

    private final Model metadataModel;
    private final UriMapping uriMapping;
    private final ConflictResolutionPolicy effectiveResolutionPolicy;
    private final ResolutionFunctionRegistry resolutionFunctionRegistry;
    private final ResolvedStatementFactoryImpl resolvedStatementFactory;
    private final Set<URI> resourceDescriptionProperties;
    private final AlternativeUriNavigator dependentPropertyMapping;

    /**
     * Creates a new instance with the given settings.
     * @param resolutionFunctionRegistry registry for obtaining conflict resolution function implementations
     * @param conflictResolutionPolicy conflict resolution parameters
     * @param uriMapping mapping of URIs to their canonical URI (based on owl:sameAs links)
     * @param metadata additional metadata for use by resolution functions (e.g. source quality etc.)
     * @param resolvedGraphsURIPrefix prefix of graph names where resolved quads are placed
     * @param resourceDescriptionProperties resource description properties, i.e. properties whose values are structured attributes
     * which should be resolved transitively
     */
    public ResourceDescriptionConflictResolverImpl(
            ResolutionFunctionRegistry resolutionFunctionRegistry,
            ConflictResolutionPolicy conflictResolutionPolicy,
            UriMapping uriMapping,
            Model metadata,
            String resolvedGraphsURIPrefix, // TODO: replace with UriGenerator class that could generate uris both for contexts and generated dependent resources
            Set<URI> resourceDescriptionProperties) {
        this.resolutionFunctionRegistry = resolutionFunctionRegistry;
        this.effectiveResolutionPolicy = ResourceDescriptionConflictResolverUtils.getEffectiveResolutionPolicy(conflictResolutionPolicy, uriMapping);
        this.dependentPropertyMapping = new AlternativeUriNavigator(ResourceDescriptionConflictResolverUtils.getDependentPropertyMapping(effectiveResolutionPolicy, uriMapping));
        this.uriMapping = uriMapping != null
                ? uriMapping
                : EmptyUriMappingIterable.getInstance();
        this.metadataModel = metadata != null
                ? metadata
                : new EmptyMetadataModel();
        this.resolvedStatementFactory = resolvedGraphsURIPrefix != null
                ? new ResolvedStatementFactoryImpl(resolvedGraphsURIPrefix)
                : new ResolvedStatementFactoryImpl(DEFAULT_RESOLVED_GRAPHS_URI_PREFIX);
        this.resourceDescriptionProperties = new HashSet<>();
        for (URI property : resourceDescriptionProperties) {
            this.resourceDescriptionProperties.add((URI) uriMapping.mapResource(property));
        }
    }

    /**
     * Apply conflict resolution process to the given resource description and return result.
     * @param resourceDescription container of quads that make up the description of the respective statement
     * (i.e. quads that are relevant for the conflict resolution process)
     * @return collection of quads derived from the input with resolved
     * conflicts, (F-)quality estimate and provenance information.
     * @throws ConflictResolutionException error during the conflict resolution process
     * @see ResolvedStatement
     */
    @Override
    public Collection<ResolvedStatement> resolveConflicts(ResourceDescription resourceDescription) throws ConflictResolutionException {
        long startTime = logStarted(resourceDescription.getDescribingStatements().size());

        ConflictClustersMap conflictClustersMap = ConflictClustersMap.fromCollection(resourceDescription.getDescribingStatements(), uriMapping);
        ResolvedResult totalResult = new ResolvedResult();

        Resource canonicalResource = uriMapping.mapResource(resourceDescription.getResource());
        Collection<ResolvedStatement> resourceResolvedStatements = resolveResource(
                conflictClustersMap.getResourceStatementsMap(canonicalResource),
                totalResult,
                conflictClustersMap);
        totalResult.addToResult(resourceResolvedStatements);

        logFinished(startTime, totalResult);
        return totalResult.getResult();
    }

    /**
     * Resolve conflicts in statements contained in {@code conflictClustersMap} for the given {@code canonicalResource}.
     * @param statementsToResolveByProperty statements to be resolved as a map canonical property -> (unmapped) statements with the property
     * @param totalResult collector of result; note that the return value is <b>not</b> added to the result when this method returns
     *      (only result of resolution of other resources within the resource description may be added)
     * @param conflictClustersMap
     * @return result of conflict resolution for the respective resource; note that the result is <b>not</b> added to {@code totalResult}
     */
    private Collection<ResolvedStatement> resolveResource(
            Map<URI, List<Statement>> statementsToResolveByProperty,
            ResolvedResult totalResult,
            ConflictClustersMap conflictClustersMap) throws ConflictResolutionException {

        Set<URI> resolvedProperties = new HashSet<>();
        Set<URI> canonicalProperties = statementsToResolveByProperty.keySet();
        Collection<ResolvedStatement> result = new ArrayList<>();
        for (URI canonicalProperty : canonicalProperties) {
            if (resolvedProperties.contains(canonicalProperty)) {
                continue;
            }

            List<URI> dependentProperties = getDependentProperties(canonicalProperty);
            if (dependentProperties == null) {
                List<Statement> conflictClusterStatements = statementsToResolveByProperty.get(canonicalProperty);
                Model conflictClusterModel = createMappedModel(conflictClusterStatements);
                Collection<ResolvedStatement> resolvedStatements = resolveConflictCluster(
                        conflictClusterModel, canonicalProperty, conflictClusterModel, conflictClustersMap, totalResult);
                result.addAll(resolvedStatements);
                resolvedProperties.add(canonicalProperty);
            } else {
                Collection<ResolvedStatement> resolvedStatements = resolveResourceDependentProperties(
                        statementsToResolveByProperty, dependentProperties, conflictClustersMap, totalResult);
                result.addAll(resolvedStatements);
                resolvedProperties.addAll(dependentProperties);
            }
        }
        return result;
    }

    // TODO: refactor + move ?
    //   (triples from the same graph should go together even if the same resource URI is used in multiple graphs)
    // FIXME: !!!! DO NOT SELECT BEST SUBJECT, BUT COMBINATION OF SUBJECT AND GRAPH

    /**
     * Resolves conflicts in {@code statementsToResolveByProperty} for a set of mutually dependent properties.
     * This method <b>doesn't strictly require statements to share the same subject or map to the same canonical subject</b> but it
     * treats the input triples as though they do map to the same canonical subject.
     * @param statementsToResolveByProperty statements to be resolved as a map canonical property -> (unmapped) statements with the property
     * @param dependentProperties list of mutually dependent properties to be resolved
     * @param conflictClustersMap
     * @param totalResult
     * @throws ConflictResolutionException CR error
     */
    private Collection<ResolvedStatement> resolveResourceDependentProperties(
            Map<URI, List<Statement>> statementsToResolveByProperty,
            List<URI> dependentProperties,
            ConflictClustersMap conflictClustersMap,
            ResolvedResult totalResult) throws ConflictResolutionException {

        // Step 1: resolve conflicts for each (non-canonical) subject and property
        Table<Resource, URI, Collection<ResolvedStatement>> conflictClustersTable = ODCSFusionToolCRUtils.newHashTable();
        for (URI property : dependentProperties) {
            List<Statement> conflictingStatements = statementsToResolveByProperty.get(property);
            if (conflictingStatements == null) {
                continue;
            }
            Collection<Statement> mappedConflictingStatements = new StatementMapper(uriMapping, VF).mapStatements(conflictingStatements);
            ClusterIterator<Statement> subjectClusterIterator = new ClusterIterator<>(conflictingStatements, StatementBySubjectComparator.getInstance());
            while (subjectClusterIterator.hasNext()) {
                List<Statement> statements = subjectClusterIterator.next();
                Resource notMappedSubject = statements.get(0).getSubject();
                Model conflictClusterModel = createMappedModel(statements);
                Collection<ResolvedStatement> resolvedConflictCluster = resolveConflictCluster(
                        conflictClusterModel, property, mappedConflictingStatements, conflictClustersMap, totalResult);
                conflictClustersTable.put(notMappedSubject, property, resolvedConflictCluster);
            }
        }

        // Step 2: Choose the best subject by aggregate quality
        Resource bestSubject = null;
        double bestSubjectQuality = -1;
        for (Resource notMappedSubject : conflictClustersTable.rowKeySet()) {
            double aggregateQualitySum = 0;
            for (URI property : dependentProperties) {
                aggregateQualitySum += ResourceDescriptionConflictResolverUtils.aggregateConflictClusterQuality(conflictClustersTable.get(notMappedSubject, property));
            }
            double notMappedSubjectQuality = aggregateQualitySum / dependentProperties.size();
            if (notMappedSubjectQuality > bestSubjectQuality) {
                bestSubject = notMappedSubject;
                bestSubjectQuality = notMappedSubjectQuality;
            }
        }

        // Step 3: Add statements for the best subject to the result
        ArrayList<ResolvedStatement> chosenStatements = new ArrayList<>();
        if (bestSubject != null) {
            Map<URI, Collection<ResolvedStatement>> selectedStatements = conflictClustersTable.row(bestSubject);
            for (Collection<ResolvedStatement> resolvedStatements : selectedStatements.values()) {
                chosenStatements.addAll(resolvedStatements);
            }
        }
        return chosenStatements;
    }

    // this method assumes that all statements in conflictClusterModel share the same subject and property

    /**
     * Resolve conflicts in {@code conflictClusterToResolve}. This methods expects that <b>all statements in
     * {@code conflictClusterToResolve} share the same subject and property</b> (no further mapping is performed).
     * @param conflictClusterToResolve statements to be resolved;
     * subjects and predicate in these triples must be the same for all triples
     * @param canonicalProperty canonical property for the conflict cluster
     * @param conflictingMappedStatements conflicting statements to be considered during quality calculation.
     * @param totalResult
     * @return resolved statements produced by conflict resolution function
     * @throws ConflictResolutionException CR error
     */
    private Collection<ResolvedStatement> resolveConflictCluster(
            Model conflictClusterToResolve,
            URI canonicalProperty,
            Collection<Statement> conflictingMappedStatements,
            ConflictClustersMap conflictClustersMap, ResolvedResult totalResult) throws ConflictResolutionException {

        if (conflictClusterToResolve.isEmpty()) {
            return Collections.emptyList();
        }

        if (resourceDescriptionProperties.contains(canonicalProperty)) {
            // TODO: extract method (or resolution function?)
            Set<Resource> objects = ResourceDescriptionConflictResolverUtils.collectResourceObjects(conflictClusterToResolve);
            Map<URI, List<Statement>> descriptionStatements = conflictClustersMap.getUnionStatementsMap(objects);
            URI newObject = generateUniqueUri();
            Collection<ResolvedStatement> resolvedStatements = resolveResource(
                    descriptionStatements,
                    totalResult,
                    conflictClustersMap);
            totalResult.addToResult(new SubjectMappingIterator(resolvedStatements.iterator(), newObject, resolvedStatementFactory)); // overwrite subjects in result

            Resource subject = conflictClusterToResolve.iterator().next().getSubject();
            double aggregateQuality = ResourceDescriptionConflictResolverUtils.aggregateConflictClusterQuality(resolvedStatements);
            return Collections.singleton(resolvedStatementFactory.create(
                    subject, canonicalProperty, newObject, aggregateQuality, ResourceDescriptionConflictResolverUtils.collectContexts(resolvedStatements)));
            // FIXME: do not return here, but continue with statements having literals as their object, treating them as non-aggregable quads
        }

        ResolutionStrategy resolutionStrategy = effectiveResolutionPolicy.getPropertyResolutionStrategies().get(canonicalProperty);
        if (resolutionStrategy == null) {
            resolutionStrategy = effectiveResolutionPolicy.getDefaultResolutionStrategy();
        }
        ResolutionFunction resolutionFunction = getResolutionFunction(resolutionStrategy);
        CRContext context = new CRContextImpl(conflictingMappedStatements, metadataModel, resolutionStrategy, resolvedStatementFactory);
        // FIXME: resolution functions generally assume that the model is spog-sorted; while this works now, it can be easily broken in future
        return resolutionFunction.resolve(conflictClusterToResolve, context);
    }


    // =======================================================================
    // Auxiliary methods
    // =======================================================================

    private URI generateUniqueUri() {
        // FIXME: prefix
        return VF.createURI(DEFAULT_RESOLVED_GRAPHS_URI_PREFIX + UUID.randomUUID());
    }

    /**
     * Apply uri mapping to given statements and create a model from them.
     * @param statements statements
     * @return model created from mapped {@code statements}
     */
    private Model createMappedModel(Iterable<Statement> statements) {
        StatementMappingIterator mappingIterator = new StatementMappingIterator(statements.iterator(), uriMapping, VF);
        return SORTED_LIST_MODEL_FACTORY.fromUnorderedIterator(mappingIterator);
    }

    protected ResolutionFunction getResolutionFunction(ResolutionStrategy resolutionStrategy) throws ResolutionFunctionNotRegisteredException {
        return resolutionFunctionRegistry.get(resolutionStrategy.getResolutionFunctionName());
    }

    private List<URI> getDependentProperties(URI property) {
        if (dependentPropertyMapping.hasAlternativeUris(property)) {
            return dependentPropertyMapping.listAlternativeUris(property);
        } else {
            return null;
        }
    }

    private long logStarted(int inputStatementCount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resolving conflicts among {} quads.", inputStatementCount);
            return System.currentTimeMillis();
        } else {
            return 0;
        }
    }

    private void logFinished(long startTime, ResolvedResult result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Conflict resolution executed in {} ms, resolved to {} quads",
                    System.currentTimeMillis() - startTime, result.getResult().size());
        }
    }

    private static class StatementBySubjectComparator implements Comparator<Statement> {
        private static final Comparator<Statement> INSTANCE = new StatementBySubjectComparator();

        public static Comparator<Statement> getInstance() {
            return INSTANCE;
        }

        @Override
        public int compare(Statement o1, Statement o2) {
            return CRUtils.compareValues(o1.getSubject(), o2.getSubject());
        }
    }
}
