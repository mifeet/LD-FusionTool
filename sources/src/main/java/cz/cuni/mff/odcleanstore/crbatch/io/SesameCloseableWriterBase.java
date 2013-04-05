/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

import com.hp.hpl.jena.graph.Triple;

import cz.cuni.mff.odcleanstore.crbatch.util.ConversionException;
import cz.cuni.mff.odcleanstore.crbatch.util.JenaToSesameConverter;


/**
 * @author Jan Michelfeit
 */
public abstract class SesameCloseableWriterBase implements CloseableRDFWriter {
    private final RDFWriter rdfWriter;
    private final Writer outputWriter;
    
    
    /**
     * Create a new instance.
     * @param outputWriter output writer
     * @param writerFactory factory for RDFWriter to be used for serialization
     * @throws IOException  I/O error
     */
    protected SesameCloseableWriterBase(Writer outputWriter, RDFWriterFactory writerFactory) throws IOException {
        this.outputWriter = outputWriter;
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
    public void write(Iterator<Triple> triples) throws IOException {
        while (triples.hasNext()) {
            write(triples.next());
        }
    }

    @Override
    public void write(Triple triple) throws IOException {
        try {
            rdfWriter.handleStatement(JenaToSesameConverter.convertTriple(triple));
        } catch (RDFHandlerException e) {
            throw new IOException(e);
        } catch (ConversionException e) {
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
