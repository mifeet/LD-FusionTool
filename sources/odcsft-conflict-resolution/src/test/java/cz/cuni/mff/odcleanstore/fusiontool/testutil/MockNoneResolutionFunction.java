package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.CRContext;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionFunction;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import org.openrdf.model.Model;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class MockNoneResolutionFunction implements ResolutionFunction {
    private AtomicInteger conflictClusterCounter = new AtomicInteger();

    @Override
    public Collection<ResolvedStatement> resolve(Model statements, CRContext crContext) throws ConflictResolutionException {
        int conflictClusterNumber = conflictClusterCounter.getAndIncrement();

        Collection<ResolvedStatement> result = new ArrayList<>();
        for (Statement statement : statements) {
            Collection<Resource> source = Collections.singleton(statement.getContext());
            double fQuality = 1.0 / crContext.getConflictingStatements().size();
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


}
