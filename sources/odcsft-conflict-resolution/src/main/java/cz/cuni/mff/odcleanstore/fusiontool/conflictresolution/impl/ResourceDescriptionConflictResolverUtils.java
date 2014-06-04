package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolutionPolicy;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ConflictResolutionPolicyImpl;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterableImpl;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;

import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

public class ResourceDescriptionConflictResolverUtils {
    static Set<Resource> collectContexts(Collection<ResolvedStatement> resolvedStatements) {
        Set<Resource> result = new HashSet<>();
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            result.addAll(resolvedStatement.getSourceGraphNames());
        }
        return result;
    }

    static UriMappingIterable getDependentPropertyMapping(ConflictResolutionPolicy effectiveResolutionPolicy, UriMapping uriMapping) {
        checkNotNull(effectiveResolutionPolicy);
        checkNotNull(uriMapping);
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

    static double aggregateConflictClusterQuality(Collection<ResolvedStatement> resolvedStatements) {
        if (resolvedStatements == null || resolvedStatements.isEmpty()) {
            return 0d;
        }
        double sum = 0d;
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            sum += resolvedStatement.getQuality();
        }
        return sum / resolvedStatements.size();
    }

    static ConflictResolutionPolicy getEffectiveResolutionPolicy(ConflictResolutionPolicy conflictResolutionPolicy, UriMapping uriMapping) {
        ResolutionStrategy effectiveDefaultStrategy = ResourceDescriptionConflictResolverImpl.DEFAULT_RESOLUTION_STRATEGY;
        Map<URI, ResolutionStrategy> effectivePropertyStrategies = new HashMap<>();

        if (conflictResolutionPolicy != null && conflictResolutionPolicy.getDefaultResolutionStrategy() != null) {
            effectiveDefaultStrategy = CRUtils.fillResolutionStrategyDefaults(
                    conflictResolutionPolicy.getDefaultResolutionStrategy(),
                    ResourceDescriptionConflictResolverImpl.DEFAULT_RESOLUTION_STRATEGY);
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
}
