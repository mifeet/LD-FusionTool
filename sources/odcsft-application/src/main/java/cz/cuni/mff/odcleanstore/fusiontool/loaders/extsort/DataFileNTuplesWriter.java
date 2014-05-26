package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.io.IOException;

/**
 * Formatter of incoming statements to the format required by the primary temporary data file.
 * Data are written to the underlying {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter} in the following format:
 * <ul>
 *     <li> S P O G for all input quads (S,P,O,G)</li>
 * </ul>
 */
public class DataFileNTuplesWriter extends RDFHandlerBase {
    private final NTuplesWriter nTuplesWriter;

    public DataFileNTuplesWriter(NTuplesWriter nTuplesWriter) {
        this.nTuplesWriter = nTuplesWriter;
    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        try {
            nTuplesWriter.writeTuple(st.getSubject(), st.getPredicate(), st.getObject(), st.getContext());
        } catch (IOException e) {
            throw new RDFHandlerException(e);
        }
    }
}
