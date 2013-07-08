/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriterFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;


/**
 * @author Jan Michelfeit
 */
public abstract class SesameCloseableRDFWriterBase implements CloseableRDFWriter {
    private final RDFHandler rdfWriter;
    private final Writer outputWriter;
    
    /**
     * Create a new instance.
     * @param outputStream output stream
     * @param writerFactory factory for RDFWriter to be used for serialization
     * @throws IOException  I/O error
     */
    protected SesameCloseableRDFWriterBase(OutputStream outputStream, RDFWriterFactory writerFactory) throws IOException {
        this.outputWriter = new BufferedWriter(new OutputStreamWriter(outputStream, "UTF-8"));
        this.rdfWriter = writerFactory.getWriter(outputWriter);
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
    public final void writeCRQuads(Iterator<ResolvedStatement> resolvedStatements) throws IOException {
        while (resolvedStatements.hasNext()) {
            write(resolvedStatements.next().getStatement());
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
        outputWriter.close();
    }
}
