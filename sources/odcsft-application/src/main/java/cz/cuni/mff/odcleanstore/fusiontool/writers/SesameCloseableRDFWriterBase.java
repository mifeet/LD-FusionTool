/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;


/**
 * Implementation of {@link CloseableRDFWriter} writing to a given Sesame {@link RDFHandler}.
 * @author Jan Michelfeit
 */
public abstract class SesameCloseableRDFWriterBase implements CloseableRDFWriter {
    private final RDFHandler rdfWriter;
    private final Closeable underlyingResource;
    
    /**
     * Create a new instance.
     * @param rdfWriter handler or written RDF data
     * @param underlyingResource a resource to be closed as soon as writing is finished
     * @throws IOException  I/O error
     */
    protected SesameCloseableRDFWriterBase(RDFHandler rdfWriter, Closeable underlyingResource) throws IOException {
        this.rdfWriter = rdfWriter;
        this.underlyingResource = underlyingResource;
        try {
            this.rdfWriter.startRDF();
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }
    
    @Override 
    public void addNamespace(String prefix, String uri) throws IOException {
        try {
            rdfWriter.handleNamespace(prefix, uri);
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }
    
    @Override
    public final void writeResolvedStatements(Iterable<ResolvedStatement> resolvedStatements) throws IOException {
        for (ResolvedStatement resolvedStatement : resolvedStatements) {
            write(resolvedStatement);
        }
    }
    
    @Override
    public final void writeQuads(Iterator<Statement> quads) throws IOException {
        while (quads.hasNext()) {
            write(quads.next());
        } 
    }

    @Override
    public final void write(Statement statement) throws IOException {
        try {
            rdfWriter.handleStatement(statement);
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            rdfWriter.endRDF();
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        }
        underlyingResource.close();
    }
}
