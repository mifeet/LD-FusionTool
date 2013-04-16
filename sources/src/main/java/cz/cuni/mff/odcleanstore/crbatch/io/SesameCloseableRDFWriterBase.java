/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;


/**
 * @author Jan Michelfeit
 */
public abstract class SesameCloseableRDFWriterBase implements CloseableRDFWriter {
    private final RDFWriter rdfWriter;
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
    public void write(Iterator<Statement> statements) throws IOException {
        while (statements.hasNext()) {
            write(statements.next());
        }
    }

    @Override
    public void write(Statement statement) throws IOException {
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
