package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ConflictResolver;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.ResolvedStatementImpl;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestConflictResolver implements ConflictResolver {
    private AtomicInteger callCounter = new AtomicInteger(0);

    private final List<Collection<Statement>> collectedStatements = new ArrayList<Collection<Statement>>();

    public List<Collection<Statement>> getCollectedStatements() {
        return collectedStatements;
    }

    @Override
    public Collection<ResolvedStatement> resolveConflicts(Iterator<Statement> statements) throws ConflictResolutionException {
        Resource nextContext = getNextContext();

        ArrayList<Statement> inputStatements = new ArrayList<Statement>();
        ArrayList<ResolvedStatement> result = new ArrayList<ResolvedStatement>();
        while (statements.hasNext()) {
            Statement statement = statements.next();
            inputStatements.add(statement);
            result.add(new ResolvedStatementImpl(statement, 1d, Collections.singleton(nextContext)));
        }
        collectedStatements.add(inputStatements);
        return result;
    }

    @Override
    public Collection<ResolvedStatement> resolveConflicts(Collection<Statement> statements) throws ConflictResolutionException {
        return resolveConflicts(statements.iterator());
    }

    private Resource getNextContext() {
        return ValueFactoryImpl.getInstance().createURI("http://" + callCounter.getAndIncrement());
    }
}
