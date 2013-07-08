package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import java.util.Collection;

import org.openrdf.model.Statement;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject).
 * @author Jan Michelfeit
 */
public interface QuadLoader extends Closeable<CRBatchException> {
    /**
     * Adds quads having the given uri or one of its owl:sameAs alternatives as their subject to quadCollestion.
     * @param uri searched subject URI
     * @param quadCollection collection to which the result will be added
     * @throws CRBatchException error
     * @see cz.cuni.mff.odcleanstore.fusiontool.DataSource#getNamedGraphRestriction()
     */
    void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws CRBatchException;
}
