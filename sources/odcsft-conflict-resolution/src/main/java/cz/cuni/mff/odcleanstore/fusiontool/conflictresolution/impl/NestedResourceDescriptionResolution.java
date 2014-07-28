package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.DummyFQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.ResolutionFunctionBase;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.NestedResourceDescriptionQualityCalculator;
import org.openrdf.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Resolution function resolving nested resource descriptions.
 * Creates a new unique URI {@code N} for the nested resource, resolves the nested resource description,
 * adds the resolved quads with {@code N} as their subject to {@link #resolverContext}.{#link ResourceDescriptionConflictResolverContext#totalResult},
 * and returns a single result quad whose object is {@code N}.
 *
 * TODO: test
 */
public class NestedResourceDescriptionResolution extends ResolutionFunctionBase {
    private static final Logger LOG = LoggerFactory.getLogger(NestedResourceDescriptionResolution.class);
    private static final String FUNCTION_NAME = "DEPENDENT_RESOURCE";

    /**
     * Returns a string identifier of this resolution function: {@value #FUNCTION_NAME}.
     * @return string identifier of this resolution function
     */
    public static String getName() {
        return FUNCTION_NAME;
    }

    private final NestedResourceDescriptionQualityCalculator nestedResourceDescriptionQualityCalculator;
    private final ResourceDescriptionConflictResolverImpl.ResourceDescriptionConflictResolverContext resolverContext;

    /**
     * Creates a new instance.
     * @param nestedResourceDescriptionQualityCalculator calculator of F-quality to be used for estimation of
     * produced {@link cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement result quads}
     * (see {@link cz.cuni.mff.odcleanstore.conflictresolution.quality.MediatingFQualityCalculator})
     */
    public NestedResourceDescriptionResolution(
            NestedResourceDescriptionQualityCalculator nestedResourceDescriptionQualityCalculator,
            ResourceDescriptionConflictResolverImpl.ResourceDescriptionConflictResolverContext resourceDescriptionConflictResolverContext) {
        super(new DummyFQualityCalculator());
        this.nestedResourceDescriptionQualityCalculator = nestedResourceDescriptionQualityCalculator;
        this.resolverContext = resourceDescriptionConflictResolverContext;
    }

    @Override
    public Collection<ResolvedStatement> resolve(Model statements, CRContext context) throws ConflictResolutionException {
        if (statements.isEmpty()) {
            return Collections.emptyList();
        }

        Set<Resource> nestedResourceSubjects = new HashSet<>(statements.size());
        boolean hasNonAggregableStatements = false;
        for (Statement statement : statements) {
            if (statement.getObject() instanceof Resource) {
                nestedResourceSubjects.add((Resource) statement.getObject());
            } else {
                hasNonAggregableStatements = true;
            }
        }

        URI canonicalProperty = context.getCanonicalProperty();
        URI newObject = resolverContext.generateUniqueUri();
        if (LOG.isTraceEnabled()) {
            LOG.trace("... resolving values of description property {} of {} (new dependent resource is {})",
                    new Object[]{canonicalProperty, context.getCanonicalSubject(), newObject});
        }
        Collection<ResolvedStatement> resolvedStatements = resolverContext.resolveNestedResource(nestedResourceSubjects, newObject);

        double aggregateQuality = nestedResourceDescriptionQualityCalculator.aggregateConflictClusterQuality(resolvedStatements);
        Collection<Resource> sources = ResourceDescriptionConflictResolverUtils.collectContexts(resolvedStatements);
        ResolvedStatement resultStatement = context.getResolvedStatementFactory().create(
                context.getCanonicalSubject(), canonicalProperty, newObject, aggregateQuality, sources);

        if (hasNonAggregableStatements) {
            return handleNonAggregableStatements(statements, context, resultStatement);
        } else {
            return Collections.singleton(resultStatement);
        }
    }

    private Collection<ResolvedStatement> handleNonAggregableStatements(Model statements, CRContext context, ResolvedStatement resultStatement) {
        ArrayList<ResolvedStatement> result = new ArrayList<>();
        result.add(resultStatement);
        for (Statement statement : statements) {
            if (!(statement.getObject() instanceof Resource)) {
                handleNonAggregableStatement(statement, statements, context, result);
            }
        }
        return result;
    }

    @Override
    protected double getNonAggregableStatementFQuality(Value value, Collection<Resource> sources, CRContext crContext) {
        return nestedResourceDescriptionQualityCalculator.getLiteralNestedResourceFQuality(value, sources, crContext);
    }
}
