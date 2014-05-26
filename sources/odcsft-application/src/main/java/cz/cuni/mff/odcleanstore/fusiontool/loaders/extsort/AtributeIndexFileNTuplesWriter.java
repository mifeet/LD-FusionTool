package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter;
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
 *     <li> O S for input quads (S,P,O,G) such that P is a resource description URI to {@code tempAttributeFile}</li>
 * </ul>
 */
public class AtributeIndexFileNTuplesWriter extends RDFHandlerBase {
    private final NTuplesWriter nTuplesWriter;
    private final Set<URI> resourceDescriptionUris;

    public AtributeIndexFileNTuplesWriter(NTuplesWriter nTuplesWriter, Set<URI> resourceDescriptionUris) {
        this.nTuplesWriter = nTuplesWriter;
        this.resourceDescriptionUris = resourceDescriptionUris;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        try {
            if (resourceDescriptionUris.contains(st.getPredicate())) {
                nTuplesWriter.writeTuple(st.getObject(), st.getSubject());
            }
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }
}
