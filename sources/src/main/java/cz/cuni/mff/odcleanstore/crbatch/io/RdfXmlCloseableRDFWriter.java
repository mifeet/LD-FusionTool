/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriterFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;

/**
 * Implementation of {@link CloseableRDFWriter} for RDF/XML output format.
 * @author Jan Michelfeit
 */
public class RdfXmlCloseableRDFWriter extends SesameCloseableRDFWriterBase {
    private static final RDFWriterFactory WRITER_FACTORY = new RDFXMLPrettyWriterFactory();
    //private static final RDFWriterFactory WRITER_FACTORY = new RDFXMLWriterFactory();
    
    
    /**
     * @param outputStream stream to which result is written
     * @throws IOException  I/O error
     */
    public RdfXmlCloseableRDFWriter(OutputStream outputStream) throws IOException {
        super(outputStream, WRITER_FACTORY);
    }

    @Override
    public void write(CRQuad crQuad) throws IOException {
        write(crQuad.getQuad());
    }
}
