package cz.cuni.mff.odcleanstore.fusiontool.writers;

import com.google.common.base.Preconditions;
import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.openrdf.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class FederatedRDFWriter implements CloseableRDFWriter {
    private static final Logger LOG = LoggerFactory.getLogger(FederatedRDFWriter.class);

    private final CloseableRDFWriter[] rdfWriters;

    public FederatedRDFWriter(List<CloseableRDFWriter> rdfWriters) {
        Preconditions.checkNotNull(rdfWriters);
        this.rdfWriters = rdfWriters.toArray(new CloseableRDFWriter[rdfWriters.size()]);
    }

    @Override
    public void writeQuads(Iterator<Statement> quads) throws IOException {
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            rdfWriter.writeQuads(quads);
        }
    }

    @Override
    public void write(Statement quad) throws IOException {
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            rdfWriter.write(quad);
        }
    }

    @Override
    public void writeResolvedStatements(Iterator<ResolvedStatement> resolvedStatements) throws IOException {
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            rdfWriter.writeResolvedStatements(resolvedStatements);
        }
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            rdfWriter.write(resolvedStatement);
        }
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            rdfWriter.addNamespace(prefix, uri);
        }
    }

    @Override
    public void close() throws IOException {
        IOException firstException = null;
        for (CloseableRDFWriter rdfWriter : rdfWriters) {
            try {
                rdfWriter.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                }
                LOG.error("Error closing RDF writer", e);
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }
}
