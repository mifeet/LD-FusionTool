package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import org.openrdf.model.Statement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class TestInputLoader implements InputLoader {
    private Iterator<Collection<Statement>> iterator;

    private final List<ResolvedStatement> collectedResolvedStatements = new ArrayList<ResolvedStatement>();

    public TestInputLoader(Collection<Collection<Statement>> statements) {
        this.iterator = statements.iterator();
    }

    public List<ResolvedStatement> getCollectedResolvedStatements() {
        return collectedResolvedStatements;
    }

    @Override
    public void initialize(URIMappingIterable uriMapping) throws ODCSFusionToolException {

    }

    @Override
    public Collection<Statement> nextQuads() throws ODCSFusionToolException {
        return iterator.next();
    }

    @Override
    public boolean hasNext() throws ODCSFusionToolException {
        return iterator.hasNext();
    }

    @Override
    public void updateWithResolvedStatements(Collection<ResolvedStatement> resolvedStatements) {
        collectedResolvedStatements.addAll(resolvedStatements);
    }

    @Override
    public void close() throws ODCSFusionToolException {
        iterator = null;
    }
}
