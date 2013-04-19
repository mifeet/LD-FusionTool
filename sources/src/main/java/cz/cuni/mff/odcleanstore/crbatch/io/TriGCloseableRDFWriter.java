/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.trig.TriGWriterFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.CRQuad;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;

/**
 * Implementation of {@link CloseableRDFWriter} for TriG output format.
 * TriG format enables writing of full {@link CRQuad} information.
 * @author Jan Michelfeit
 */
public class TriGCloseableRDFWriter extends SesameCloseableRDFWriterBase {
    private static final RDFWriterFactory WRITER_FACTORY = new TriGWriterFactory();
    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
    private static final URI QUALITY_PROPERTY = VALUE_FACTORY.createURI(ODCS.quality);
    private static final URI SOURCE_GRAPH_PROPERTY = VALUE_FACTORY.createURI(ODCS.sourceGraph);
    
    private final URI metadataGraphURI;
    
    /**
     * @param outputStream writer to which result is written
     * @param metadataGraphURI URI of named graph where CR metadata (quality, sources) will be placed
     * @throws IOException  I/O error
     */
    public TriGCloseableRDFWriter(OutputStream outputStream, URI metadataGraphURI) throws IOException {
        super(outputStream, WRITER_FACTORY);
        this.metadataGraphURI = metadataGraphURI;
    }
    
    @Override
    public void write(CRQuad crQuad) throws IOException {
        write(crQuad.getQuad());
        if (metadataGraphURI != null) {
            write(VALUE_FACTORY.createStatement(
                    crQuad.getQuad().getContext(),
                    QUALITY_PROPERTY,
                    VALUE_FACTORY.createLiteral(crQuad.getQuality()),
                    metadataGraphURI));
            for (String sourceNamedGraph : crQuad.getSourceNamedGraphURIs()) {
                write(VALUE_FACTORY.createStatement(
                        crQuad.getQuad().getContext(),
                        SOURCE_GRAPH_PROPERTY,
                        VALUE_FACTORY.createURI(sourceNamedGraph),
                        metadataGraphURI));
            }
        }
    }
}
