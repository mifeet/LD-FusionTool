package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunction;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MockBestResolutionFunctionWithQuality implements ResolutionFunction {
    private final Map<Resource, Double> qualityMap;
    private AtomicInteger conflictClusterCounter = new AtomicInteger();

    public static MockBestResolutionFunctionWithQuality newResolutionFunction() {
        return new MockBestResolutionFunctionWithQuality(new HashMap<Resource, Double>());
    }

    public MockBestResolutionFunctionWithQuality withQuality(Resource context, double quality) {
        this.qualityMap.put(context, quality);
        return this;
    }

    public MockBestResolutionFunctionWithQuality(Map<Resource, Double> qualityMap) {
        this.qualityMap = qualityMap;
    }

    @Override
    public Collection<ResolvedStatement> resolve(Model statements, CRContext crContext) throws ConflictResolutionException {
        int conflictClusterNumber = conflictClusterCounter.getAndIncrement();

        ResolvedStatement bestResolvedStatement = null;
        double bestQuality = -1;
        for (Statement statement : statements) {
            Collection<Resource> source = Collections.singleton(statement.getContext());
            double fQuality = getQuality(statement);
            if (fQuality > bestQuality) {
                 bestResolvedStatement = new MockResolvedStatement(
                        crContext.getResolvedStatementFactory().create(statement, fQuality, source).getStatement(),
                        fQuality,
                        source,
                        conflictClusterNumber,
                        crContext.getResolutionStrategy(),
                        crContext.getConflictingStatements());
            }
        }
        if (bestResolvedStatement == null) {
            return Collections.emptySet();
        } else {
            return Collections.singleton(bestResolvedStatement);
        }
    }

    protected double getQuality(Statement statement) {
        if (qualityMap.containsKey(statement.getContext())) {
            return qualityMap.get(statement.getContext());
        } else {
            return 0;
        }
    }
}
