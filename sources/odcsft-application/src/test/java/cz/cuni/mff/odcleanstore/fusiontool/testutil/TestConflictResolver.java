package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestConflictResolver implements ResourceDescriptionConflictResolver {
    private AtomicInteger callCounter = new AtomicInteger(0);

    private final List<Collection<Statement>> collectedStatements = new ArrayList<Collection<Statement>>();

    public List<Collection<Statement>> getCollectedStatements() {
        return collectedStatements;
    }

    @Override
    public Collection<ResolvedStatement> resolveConflicts(ResourceDescription resourceDescription) throws ConflictResolutionException {
        Resource nextContext = getNextContext();

        ArrayList<Statement> inputStatements = new ArrayList<Statement>();
        ArrayList<ResolvedStatement> result = new ArrayList<ResolvedStatement>();
        for (Statement statement : resourceDescription.getDescribingStatements()) {
            inputStatements.add(statement);
            result.add(new ResolvedStatementImpl(statement, 1d, Collections.singleton(nextContext)));
        }
        collectedStatements.add(inputStatements);
        return result;
    }

    private Resource getNextContext() {
        return ValueFactoryImpl.getInstance().createURI("http://" + callCounter.getAndIncrement());
    }
}
