package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Collection;

public class MockResolvedStatement extends ResolvedStatementImpl {
    private final int conflictClusterNumber;
    private final ResolutionStrategy resolutionStrategy;
    private final Collection<Statement> conflictingStatements;

    public MockResolvedStatement(Statement statement, double quality, Collection<Resource> sourceGraphNames,
            int conflictClusterNumber, ResolutionStrategy resolutionStrategy, Collection<Statement> conflictingStatements) {
        super(statement, quality, sourceGraphNames);
        this.conflictClusterNumber = conflictClusterNumber;
        this.resolutionStrategy = resolutionStrategy;
        this.conflictingStatements = conflictingStatements;
    }

    public int getConflictClusterNumber() {
        return conflictClusterNumber;
    }

    public ResolutionStrategy getResolutionStrategy() {
        return resolutionStrategy;
    }

    public Collection<Statement> getConflictingStatements() {
        return conflictingStatements;
    }
}
