package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import org.openrdf.model.Statement;

import java.util.Collection;

/**
 * Loads triples containing statements about a given URI resource (having the URI as their subject).
 * @author Jan Michelfeit
 */
public interface ResourceQuadLoader extends Closeable<ODCSFusionToolException> {
    /**
     * Adds quads having the given uri or one of its owl:sameAs alternatives as their subject to quadCollection.
     * @param uri searched subject URI
     * @param quadCollection collection to which the result will be added
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException error
     * @see cz.cuni.mff.odcleanstore.fusiontool.source.DataSource#getNamedGraphRestriction()
     */
    void loadQuadsForURI(String uri, Collection<Statement> quadCollection) throws ODCSFusionToolException;
}
