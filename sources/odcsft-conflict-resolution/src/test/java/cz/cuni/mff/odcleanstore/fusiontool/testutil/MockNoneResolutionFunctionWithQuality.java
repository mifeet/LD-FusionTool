package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunction;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNoneResolutionFunctionWithQuality implements ResolutionFunction {
    private final Map<Resource, Double> qualityMap;
    private AtomicInteger conflictClusterCounter = new AtomicInteger();

    public static MockNoneResolutionFunctionWithQuality newResolutionFunction() {
        return new MockNoneResolutionFunctionWithQuality(new HashMap<Resource, Double>());
    }

    public MockNoneResolutionFunctionWithQuality withQuality(Resource context, double quality) {
        this.qualityMap.put(context, quality);
        return this;
    }

    public MockNoneResolutionFunctionWithQuality(Map<Resource, Double> qualityMap) {
        this.qualityMap = qualityMap;
    }

    @Override
    public Collection<ResolvedStatement> resolve(Model statements, CRContext crContext) throws ConflictResolutionException {
        int conflictClusterNumber = conflictClusterCounter.getAndIncrement();

        Collection<ResolvedStatement> result = new ArrayList<>();
        for (Statement statement : statements) {
            Collection<Resource> source = Collections.singleton(statement.getContext());
            double fQuality = getQuality(statement);
            ResolvedStatement resolvedStatement = new MockResolvedStatement(
                    crContext.getResolvedStatementFactory().create(statement, fQuality, source).getStatement(),
                    fQuality,
                    source,
                    conflictClusterNumber,
                    crContext.getResolutionStrategy(),
                    crContext.getConflictingStatements());
            result.add(resolvedStatement);
        }
        return result;
    }

    protected double getQuality(Statement statement) {
        if (qualityMap.containsKey(statement.getContext())) {
            return qualityMap.get(statement.getContext());
        } else {
            return 0;
        }
    }
}
