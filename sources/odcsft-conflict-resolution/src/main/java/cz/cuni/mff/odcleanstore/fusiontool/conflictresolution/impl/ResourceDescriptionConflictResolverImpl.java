package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import com.google.common.collect.Table;
import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolutionPolicy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumCardinality;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunction;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunctionRegistry;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ResolutionFunctionNotRegisteredException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.CRContextImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolutionStrategyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementFactoryImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.AllResolution;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.EmptyUriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.ClusterIterator;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util.ODCSFusionToolCRUtils;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final ConflictResolutionPolicy conflictResolutionPolicy;
    private final ResolutionFunctionRegistry resolutionFunctionRegistry;
    private final ResolvedStatementFactoryImpl resolvedStatementFactory;

    /**
     * Creates a new instance with the given settings.
     * @param resolutionFunctionRegistry registry for obtaining conflict resolution function implementations
     * @param conflictResolutionPolicy conflict resolution parameters
     * @param uriMapping mapping of URIs to their canonical URI (based on owl:sameAs links)
     * @param metadata additional metadata for use by resolution functions (e.g. source quality etc.)
     * @param resolvedGraphsURIPrefix prefix of graph names where resolved quads are placed
     */
    public ResourceDescriptionConflictResolverImpl(
            ResolutionFunctionRegistry resolutionFunctionRegistry,
            ConflictResolutionPolicy conflictResolutionPolicy,
            UriMapping uriMapping,
            Model metadata,
            String resolvedGraphsURIPrefix) {
        this.resolutionFunctionRegistry = resolutionFunctionRegistry;
        this.conflictResolutionPolicy = conflictResolutionPolicy;
        this.uriMapping = uriMapping != null
                ? uriMapping
                : EmptyUriMapping.getInstance();
        this.metadataModel = metadata != null
                ? metadata
                : new EmptyMetadataModel();
        this.resolvedStatementFactory = resolvedGraphsURIPrefix != null
                ? new ResolvedStatementFactoryImpl(resolvedGraphsURIPrefix)
                : new ResolvedStatementFactoryImpl(DEFAULT_RESOLVED_GRAPHS_URI_PREFIX);
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

        ConflictResolutionPolicy effectiveResolutionPolicy = getEffectiveResolutionPolicy();
        ConflictClustersMap conflictClustersMap = ConflictClustersMap.fromCollection(resourceDescription.getDescribingStatements(), uriMapping);
        Collection<ResolvedStatement> result = createResultCollection(resourceDescription.getDescribingStatements().size());
        AlternativeUriNavigator dependentProperties = new AlternativeUriNavigator(getDependentPropertyMapping(effectiveResolutionPolicy));

        Resource canonicalResource = uriMapping.mapResource(resourceDescription.getResource());
        resolveResource(canonicalResource, conflictClustersMap, dependentProperties, effectiveResolutionPolicy, result);

        logFinished(startTime, result);
        return result;
    }

    /**
     * Resolve conflicts in statements contained in {@code conflictClustersMap} for the given {@code canonicalResource}.
     * @param canonicalResource canonical version of resource to be resolved
     * @param conflictClustersMap conflictClustersMap statements grouped by canonical subject and property
     * @param dependentPropertyMapping property dependencies
     * @param effectiveResolutionPolicy conflict resolution policy to be used
     * @param result collection where the resolved result is added to
     */
    private void resolveResource(
            Resource canonicalResource,
            ConflictClustersMap conflictClustersMap,
            AlternativeUriNavigator dependentPropertyMapping,
            ConflictResolutionPolicy effectiveResolutionPolicy,
            Collection<ResolvedStatement> result) throws ConflictResolutionException {

        Iterator<URI> propertyIt = conflictClustersMap.listProperties(canonicalResource);
        Set<URI> resolvedProperties = new HashSet<>();
        while (propertyIt.hasNext()) {
            URI property = propertyIt.next();
            if (resolvedProperties.contains(property)) {
                continue;
            }

            if (dependentPropertyMapping.hasAlternativeUris(property)) {
                List<URI> dependentProperties = dependentPropertyMapping.listAlternativeUris(property);
                resolveResourceDependentProperties(
                        canonicalResource,
                        dependentProperties,
                        conflictClustersMap.getResourceStatementsMap(canonicalResource),
                        effectiveResolutionPolicy,
                        result);
                resolvedProperties.addAll(dependentProperties);
            } else {
                resolveResourceProperty(
                        canonicalResource,
                        property,
                        conflictClustersMap.getConflictClusterStatements(canonicalResource, property),
                        effectiveResolutionPolicy,
                        result);
                resolvedProperties.add(property);
            }
        }
    }

    // TODO: refactor - move ?
    // FIXME: !!!! DO NOT SELECT BEST SUBJECT, BUT COMBINATION OF SUBJECT AND GRAPH
    //   (triples from the same graph should go together even if the same resource URI is used in multiple graphs)
    private void resolveResourceDependentProperties(
            Resource subject,
            List<URI> dependentProperties,
            Map<URI, List<Statement>> statementsByProperty,
            ConflictResolutionPolicy effectiveResolutionPolicy,
            Collection<ResolvedStatement> result) throws ConflictResolutionException {

        // Step 1: resolve conflicts for each (non-canonical) subject and property
        Table<Resource, URI, Collection<ResolvedStatement>> conflictClustersTable = ODCSFusionToolCRUtils.newHashTable();
        for (URI property : dependentProperties) {
            List<Statement> conflictingStatements = statementsByProperty.get(property);
            if (conflictingStatements == null) {
                continue;
            }
            ClusterIterator<Statement> subjectClusterIterator = new ClusterIterator<>(conflictingStatements, StatementBySubjectComparator.getInstance());
            while (subjectClusterIterator.hasNext()) {
                List<Statement> statements = subjectClusterIterator.next();
                Resource notMappedSubject = statements.get(0).getSubject();
                StatementMappingIterator mappingIterator = new StatementMappingIterator(statements.iterator(), uriMapping, VF, true, true, true);
                Model conflictClusterModel = SORTED_LIST_MODEL_FACTORY.fromUnorderedIterator(mappingIterator);
                Collection<ResolvedStatement> resolvedConflictCluster = resolveConflictCluster(
                        notMappedSubject, property, conflictClusterModel, effectiveResolutionPolicy, conflictingStatements);
                conflictClustersTable.put(notMappedSubject, property, resolvedConflictCluster);
            }
        }

        // Step 2: Choose the best subject by aggregate quality
        Resource bestSubject = null;
        double bestSubjectQuality = -1;
        for (Resource notMappedSubject : conflictClustersTable.rowKeySet()) {
            double aggregateQualitySum = 0;
            for (URI property : dependentProperties) {
                aggregateQualitySum += aggregateConflictClusterQuality(conflictClustersTable.get(notMappedSubject, property));
            }
            double notMappedSubjectQuality = aggregateQualitySum / dependentProperties.size();
            if (notMappedSubjectQuality > bestSubjectQuality) {
                bestSubject = notMappedSubject;
                bestSubjectQuality = notMappedSubjectQuality;
            }
        }

        // Step 3: Add statements for the best subject to the result
        if (bestSubject != null) {
            Map<URI, Collection<ResolvedStatement>> selectedStatements = conflictClustersTable.row(bestSubject);
            for (Collection<ResolvedStatement> resolvedStatements : selectedStatements.values()) {
                result.addAll(resolvedStatements);
            }
        }
    }

    private double aggregateConflictClusterQuality(Collection<ResolvedStatement> resolvedStatements) {
        if (resolvedStatements == null || resolvedStatements.isEmpty()) {
            return 0d;
        }
        double sum = 0d;
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            sum += resolvedStatement.getQuality();
        }
        return sum / resolvedStatements.size();
    }

    private void resolveResourceProperty(
            Resource subject,
            URI property,
            List<Statement> conflictClusterStatements,
            ConflictResolutionPolicy effectiveResolutionPolicy,
            Collection<ResolvedStatement> result) throws ConflictResolutionException {

        StatementMappingIterator mappingIterator = new StatementMappingIterator(
                conflictClusterStatements.iterator(), uriMapping, VF, true, true, true);
        Model conflictClusterModel = SORTED_LIST_MODEL_FACTORY.fromUnorderedIterator(mappingIterator);
        result.addAll(resolveConflictCluster(subject, property, conflictClusterModel, effectiveResolutionPolicy, conflictClusterModel));
    }

    // this method assumes that all statements in conflictClusterModel share the same subject and property
    private Collection<ResolvedStatement> resolveConflictCluster(
            Resource subject,
            URI property,
            Model conflictClusterModel,
            ConflictResolutionPolicy effectiveResolutionPolicy,
            Collection<Statement> conflictingStatements) throws ConflictResolutionException {

        if (conflictClusterModel.isEmpty()) {
            return Collections.emptyList();
        }

        // Get resolution strategy
        ResolutionStrategy resolutionStrategy = effectiveResolutionPolicy.getPropertyResolutionStrategies().get(property);
        if (resolutionStrategy == null) {
            resolutionStrategy = effectiveResolutionPolicy.getDefaultResolutionStrategy();
        }

        // Prepare resolution functions & context
        ResolutionFunction resolutionFunction = getResolutionFunction(resolutionStrategy);
        CRContext context = new CRContextImpl(conflictingStatements, metadataModel, resolutionStrategy, resolvedStatementFactory);

        // Resolve conflicts & append to result
        return resolutionFunction.resolve(conflictClusterModel, context);
    }

    private ConflictResolutionPolicy getEffectiveResolutionPolicy() {
        ResolutionStrategy effectiveDefaultStrategy = DEFAULT_RESOLUTION_STRATEGY;
        Map<URI, ResolutionStrategy> effectivePropertyStrategies = new HashMap<>();

        if (conflictResolutionPolicy != null && conflictResolutionPolicy.getDefaultResolutionStrategy() != null) {
            effectiveDefaultStrategy = CRUtils.fillResolutionStrategyDefaults(
                    conflictResolutionPolicy.getDefaultResolutionStrategy(),
                    DEFAULT_RESOLUTION_STRATEGY);
        }

        if (conflictResolutionPolicy != null && conflictResolutionPolicy.getPropertyResolutionStrategies() != null) {
            for (Map.Entry<URI, ResolutionStrategy> entry : conflictResolutionPolicy.getPropertyResolutionStrategies().entrySet()) {
                URI mappedURI = (URI) uriMapping.mapResource(entry.getKey());
                ResolutionStrategy strategy = CRUtils.fillResolutionStrategyDefaults(entry.getValue(), effectiveDefaultStrategy);
                effectivePropertyStrategies.put(mappedURI, strategy);
            }
        }

        ConflictResolutionPolicyImpl result = new ConflictResolutionPolicyImpl();
        result.setDefaultResolutionStrategy(effectiveDefaultStrategy);
        result.setPropertyResolutionStrategy(effectivePropertyStrategies);
        return result;
    }

    private UriMappingIterable getDependentPropertyMapping(ConflictResolutionPolicy effectiveResolutionPolicy) {
        UriMappingIterableImpl dependentPropertyMapping = new UriMappingIterableImpl();
        for (URI uri : effectiveResolutionPolicy.getPropertyResolutionStrategies().keySet()) {
            ResolutionStrategy resolutionStrategy = effectiveResolutionPolicy.getPropertyResolutionStrategies().get(uri);
            if (resolutionStrategy.getDependsOn() != null) {
                dependentPropertyMapping.addLink(
                        (URI) uriMapping.mapResource(uri),
                        (URI) uriMapping.mapResource(resolutionStrategy.getDependsOn()));
            }
        }
        return dependentPropertyMapping;
    }

    protected ResolutionFunction getResolutionFunction(ResolutionStrategy resolutionStrategy) throws ResolutionFunctionNotRegisteredException {
        return resolutionFunctionRegistry.get(resolutionStrategy.getResolutionFunctionName());
    }

    protected Collection<ResolvedStatement> createResultCollection(int inputSize) {
        return new ArrayList<>(inputSize / 2);
    }

    private long logStarted(int inputStatementCount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resolving conflicts among {} quads.", inputStatementCount);
            return System.currentTimeMillis();
        } else {
            return 0;
        }
    }

    private void logFinished(long startTime, Collection<ResolvedStatement> result) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Conflict resolution executed in {} ms, resolved to {} quads",
                    System.currentTimeMillis() - startTime, result.size());
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
