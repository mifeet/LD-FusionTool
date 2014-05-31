package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Collection;

public class MockResolvedStatement extends ResolvedStatementImpl {
    private final int conflictClusterNumber;
    private final ResolutionStrategy resolutionStrategy;

    public MockResolvedStatement(
            Statement statement, double quality, Collection<Resource> sourceGraphNames, int conflictClusterNumber, ResolutionStrategy resolutionStrategy) {
        super(statement, quality, sourceGraphNames);
        this.conflictClusterNumber = conflictClusterNumber;
        this.resolutionStrategy = resolutionStrategy;
    }

    public int getConflictClusterNumber() {
        return conflictClusterNumber;
    }

    public ResolutionStrategy getResolutionStrategy() {
        return resolutionStrategy;
    }
}
