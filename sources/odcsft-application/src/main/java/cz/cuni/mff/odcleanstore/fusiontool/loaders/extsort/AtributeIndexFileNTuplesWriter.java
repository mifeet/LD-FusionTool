package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.IOException;
import java.util.Set;

/**
 * Formatter of incoming statements to the format used in the attribute index temporary file.
 * Data are written to the underlying {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter} in the following format:
 * <ul>
 *     <li> c(O) c(S) for input quads (S,P,O,G) such that P is a resource description URI to {@code tempAttributeFile} and O is a {@link org.openrdf.model.Resource}</li>
 * </ul>
 * where c(x) is the canonical version of x.
 */
public class AtributeIndexFileNTuplesWriter extends RDFHandlerBase {
    private final NTuplesWriter nTuplesWriter;
    private final Set<URI> resourceDescriptionUris;
    private final UriMapping uriMapping;

    public AtributeIndexFileNTuplesWriter(NTuplesWriter nTuplesWriter, Set<URI> resourceDescriptionUris, UriMapping uriMapping) {
        this.nTuplesWriter = nTuplesWriter;
        this.resourceDescriptionUris = resourceDescriptionUris;
        this.uriMapping = uriMapping;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        try {
            if (st.getObject() instanceof Resource && resourceDescriptionUris.contains(st.getPredicate())) {
                nTuplesWriter.writeTuple(
                        uriMapping.mapResource((Resource) st.getObject()),
                        uriMapping.mapResource(st.getSubject()));
            }
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }
}
