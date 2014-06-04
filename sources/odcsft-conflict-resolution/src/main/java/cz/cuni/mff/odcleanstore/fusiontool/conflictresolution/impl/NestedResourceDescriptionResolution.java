package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.quality.MediatingFQualityCalculator;
import cz.cuni.mff.odcleanstore.conflictresolution.resolution.MediatingResolutionFunction;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Resolution function resolving nested resource descriptions.
 * Creates a new unique URI {@code N} for the nested resource, resolves the nested resource description,
 * adds the resolved quads with {@code N} as their subject to {@link #resolverContext}.{#link ResourceDescriptionConflictResolverContext#totalResult},
 * and returns a single result quad whose object is {@code N}.
 */
class NestedResourceDescriptionResolution extends MediatingResolutionFunction {
    private static final Logger LOG = LoggerFactory.getLogger(NestedResourceDescriptionResolution.class);
    private final ResourceDescriptionConflictResolverImpl.ResourceDescriptionConflictResolverContext resolverContext;

    /**
     * Creates a new instance.
     * @param fQualityCalculator calculator of F-quality to be used for estimation of
     * produced {@link cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement result quads}
     * (see {@link cz.cuni.mff.odcleanstore.conflictresolution.quality.MediatingFQualityCalculator})
     */
    public NestedResourceDescriptionResolution(
            MediatingFQualityCalculator fQualityCalculator,
            ResourceDescriptionConflictResolverImpl.ResourceDescriptionConflictResolverContext resourceDescriptionConflictResolverContext) {
        super(fQualityCalculator);
        resolverContext = resourceDescriptionConflictResolverContext;
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
        LOG.trace("... resolving values of description property {} (new dependent resource is {})", canonicalProperty, newObject);
        Collection<ResolvedStatement> resolvedStatements = resolverContext.resolveNestedResource(nestedResourceSubjects, newObject);

        double aggregateQuality = ResourceDescriptionConflictResolverUtils.aggregateConflictClusterQuality(resolvedStatements);
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
}
