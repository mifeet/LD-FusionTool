/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.IOException;
import java.io.OutputStream;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.trig.TriGWriterFactory;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
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
    
    private final URI dataContext;
    private final URI metadataContext;
    
    /**
     * @param outputStream writer to which result is written
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs)
     * @throws IOException  I/O error
     */
    public TriGCloseableRDFWriter(OutputStream outputStream, URI dataContext, URI metadataContext) throws IOException {
        super(outputStream, WRITER_FACTORY);
        this.dataContext = dataContext;
        this.metadataContext = metadataContext;
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        if (dataContext == null) {
            write(resolvedStatement.getStatement());
            if (metadataContext != null) {
                write(VALUE_FACTORY.createStatement(
                        resolvedStatement.getStatement().getContext(),
                        QUALITY_PROPERTY,
                        VALUE_FACTORY.createLiteral(resolvedStatement.getQuality()),
                        metadataContext));
                for (Resource sourceNamedGraph : resolvedStatement.getSourceGraphNames()) {
                    write(VALUE_FACTORY.createStatement(
                            resolvedStatement.getStatement().getContext(),
                            SOURCE_GRAPH_PROPERTY,
                            sourceNamedGraph,
                            metadataContext));
                }
            }
        } else {
            Statement statement = resolvedStatement.getStatement();
            write(VALUE_FACTORY.createStatement(
                    statement.getSubject(),
                    statement.getPredicate(),
                    statement.getObject(),
                    dataContext));
        }
    }
}
