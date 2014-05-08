/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.vocabulary.ODCS;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFHandler;

import java.io.Closeable;
import java.io.IOException;

/**
 * Implementation of {@link CloseableRDFWriter} writing to a given Sesame {@link RDFHandler} with support for named graphs. The
 * given dataContext and metadataContext determine in which named graph
 * and whether the data and metadata are written.
 * @author Jan Michelfeit
 */
public class SesameCloseableRDFWriterQuad extends SesameCloseableRDFWriterBase {
    private final URI dataContext;
    private final URI metadataContext;

    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
    
    /**
     * @param rdfWriter handler or written RDF data
     * @param underlyingResource a resource to be closed as soon as writing is finished
     * @param dataContext URI of named graph where resolved quads will be placed or null for unique graph per quad
     *        (if the serialization format supports named graphs)
     * @param metadataContext URI of named graph where CR metadata will be placed or null for no metadata
     *        (if the serialization format supports named graphs)
     * @throws IOException  I/O error
     */
    public SesameCloseableRDFWriterQuad(RDFHandler rdfWriter, Closeable underlyingResource, URI dataContext, URI metadataContext) 
            throws IOException {
        
        super(rdfWriter, underlyingResource);
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
                        ODCS.QUALITY,
                        VALUE_FACTORY.createLiteral(resolvedStatement.getQuality()),
                        metadataContext));
                for (Resource sourceNamedGraph : resolvedStatement.getSourceGraphNames()) {
                    write(VALUE_FACTORY.createStatement(
                            resolvedStatement.getStatement().getContext(),
                            ODCS.SOURCE_GRAPH,
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
