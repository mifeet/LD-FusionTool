package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl.ResourceDescriptionImpl;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
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
    public void initialize(UriMappingIterable uriMapping) throws ODCSFusionToolException {

    }

    @Override
    public ResourceDescription next() throws ODCSFusionToolException {
        Collection<Statement> statements = iterator.next();
        if (statements.isEmpty()) {
            return new ResourceDescriptionImpl(null, statements);
        } else {
            return new ResourceDescriptionImpl(statements.iterator().next().getSubject(), statements);
        }
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
