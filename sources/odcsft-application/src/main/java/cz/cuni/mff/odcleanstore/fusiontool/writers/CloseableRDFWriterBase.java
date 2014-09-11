package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.openrdf.model.Statement;

import java.io.IOException;
import java.util.Iterator;

public abstract class CloseableRDFWriterBase implements CloseableRDFWriter {
    @Override
    public void writeQuads(Iterator<Statement> quads) throws IOException {
        while (quads.hasNext()) {
            write(quads.next());
        }
    }

    @Override
    public void writeResolvedStatements(Iterable<ResolvedStatement> resolvedStatements) throws IOException {
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            write(resolvedStatement);
        }
    }
}
