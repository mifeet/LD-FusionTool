package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import org.openrdf.model.Statement;

import java.io.IOException;
import java.util.*;

public class TestRDFWriter implements CloseableRDFWriter {

    public final List<Statement> collectedStatements = new ArrayList<Statement>();

    public final List<ResolvedStatement> collectedResolvedStatements = new ArrayList<ResolvedStatement>();

    public final Map<String, String> namespaces = new HashMap<String, String>();

    public List<Statement> getCollectedStatements() {
        return collectedStatements;
    }

    public List<ResolvedStatement> getCollectedResolvedStatements() {
        return collectedResolvedStatements;
    }

    public Map<String, String> getNamespaces() {
        return namespaces;
    }

    @Override
    public void writeQuads(Iterator<Statement> quads) throws IOException {
        while (quads.hasNext()) {
            write(quads.next());
        }
    }

    @Override
    public void write(Statement quad) throws IOException {
        collectedStatements.add(quad);
    }

    @Override
    public void writeResolvedStatements(Iterator<ResolvedStatement> resolvedStatements) throws IOException {
        while (resolvedStatements.hasNext()) {
            write(resolvedStatements.next());
        }
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        collectedResolvedStatements.add(resolvedStatement);
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        namespaces.put(prefix, uri);
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
