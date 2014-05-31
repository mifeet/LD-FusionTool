package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.*;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.*;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.EmptyMetadataModel;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.AllResolution;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
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

    private final Model metadataModel;
    private final URIMapping uriMapping;
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
            URIMapping uriMapping,
            Model metadata,
            String resolvedGraphsURIPrefix) {
        this.resolutionFunctionRegistry = resolutionFunctionRegistry;
        this.conflictResolutionPolicy = conflictResolutionPolicy;
        this.uriMapping = uriMapping != null
                ? uriMapping
                : EmptyURIMapping.getInstance();
        this.metadataModel = metadata != null
                ? metadata
                : new EmptyMetadataModel();
        this.resolvedStatementFactory = resolvedGraphsURIPrefix != null
                ? new ResolvedStatementFactoryImpl(resolvedGraphsURIPrefix)
                : new ResolvedStatementFactoryImpl(DEFAULT_RESOLVED_GRAPHS_URI_PREFIX);
    }

    @Override
    public Collection<ResolvedStatement> resolveConflicts(ResourceDescription resourceDescription) throws ConflictResolutionException {
        final Resource resolvedResource = resourceDescription.getResource();
        final Statement[] inputStatements = resourceDescription.getDescribingStatements().toArray(
                new Statement[resourceDescription.getDescribingStatements().size()]);
        long startTime = logStarted(inputStatements);

        // Prepare effective resolution strategy based on per-predicate strategies, default strategy & uri mappings
        ConflictResolutionPolicy effectiveResolutionPolicy = getEffectiveResolutionPolicy();

        // Apply owl:sameAs mappings, remove duplicities, sort into clusters of conflicting quads
        ConflictClustersCollection conflictClusters = new ConflictClustersCollection(inputStatements, uriMapping, resolvedStatementFactory.getValueFactory());

        // Resolve conflicts:
        Collection<ResolvedStatement> result = createResultCollection(inputStatements.length);
        //resolveResource(resourceDescription.getResource(), context, result);

        logFinished(startTime, result);
        return result;
    }

    /**
     * Resolve conflicts in statements contained in {@code context} for the given {@code resource}.
     * @param resource resource to be resolved
     * @param context context information for conflict resolution
     * @param result collection where the resolved result is added to
     */
    private void resolveResource(Resource resource, CRContext context, Collection<ResolvedStatement> result) {

    }

    private Collection<ResolvedStatement> createResultCollection(int inputSize) {
        return new ArrayList<ResolvedStatement>(inputSize / 2);
    }

    private ConflictResolutionPolicy getEffectiveResolutionPolicy() {
        ResolutionStrategy effectiveDefaultStrategy = DEFAULT_RESOLUTION_STRATEGY;
        Map<URI, ResolutionStrategy> effectivePropertyStrategies = new HashMap<URI, ResolutionStrategy>();

        if (conflictResolutionPolicy != null && conflictResolutionPolicy.getDefaultResolutionStrategy() != null) {
            effectiveDefaultStrategy = CRUtils.fillResolutionStrategyDefaults(
                    conflictResolutionPolicy.getDefaultResolutionStrategy(),
                    DEFAULT_RESOLUTION_STRATEGY);
        }

        if (conflictResolutionPolicy != null && conflictResolutionPolicy.getPropertyResolutionStrategies() != null) {
            for (Map.Entry<URI, ResolutionStrategy> entry : conflictResolutionPolicy.getPropertyResolutionStrategies().entrySet()) {
                URI mappedURI = uriMapping.mapURI(entry.getKey());
                ResolutionStrategy strategy = CRUtils.fillResolutionStrategyDefaults(entry.getValue(), effectiveDefaultStrategy);
                effectivePropertyStrategies.put(mappedURI, strategy);
            }
        }

        ConflictResolutionPolicyImpl result = new ConflictResolutionPolicyImpl();
        result.setDefaultResolutionStrategy(effectiveDefaultStrategy);
        result.setPropertyResolutionStrategy(effectivePropertyStrategies);
        return result;
    }

    // TODO: move to a separate class?
    private Collection<ResolvedStatement> resolveConflictCluster(
            Resource subject,
            URI predicate,
            Model conflictClusterModel,
            ConflictResolutionPolicy effectiveResolutionPolicy,
            Collection<Statement> conflictingStatements) throws ConflictResolutionException {

        if (conflictClusterModel.isEmpty()) {
            return Collections.emptyList();
        }

        // Get resolution strategy
        ResolutionStrategy resolutionStrategy = effectiveResolutionPolicy.getPropertyResolutionStrategies().get(predicate);
        if (resolutionStrategy == null) {
            resolutionStrategy = effectiveResolutionPolicy.getDefaultResolutionStrategy();
        }

        // Prepare resolution functions & context
        ResolutionFunction resolutionFunction = resolutionFunctionRegistry.get(resolutionStrategy.getResolutionFunctionName());
        CRContext context = new CRContextImpl(conflictingStatements, metadataModel, resolutionStrategy, resolvedStatementFactory);

        // Resolve conflicts & append to result
        return resolutionFunction.resolve(conflictClusterModel, context);
    }

    private long logStarted(Statement[] inputStatements) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resolving conflicts among {} quads.", inputStatements.length);
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
}
