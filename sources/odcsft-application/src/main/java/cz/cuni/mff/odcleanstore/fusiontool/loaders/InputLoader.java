package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.urimapping.URIMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;

import java.util.Collection;

/**
 * Loader of input quads to be resolved.
 * Implementors can themselves control how quads are retrieved, how many are returned in each batch.
 * All quads sharing the same subject, however, are guaranteed to be returned in a single batch of quads
 * returned by {@link #nextQuads()}.
 */
public interface InputLoader extends Closeable<ODCSFusionToolException> {
    /**
     * Perform any initialization. This operation can be expensive.
     * More demanding initialization should be done here instead of in the constructor.
     * @param uriMapping canonical URI mapping
     * @throws ODCSFusionToolException error
     */
    void initialize(URIMappingIterable uriMapping) throws ODCSFusionToolException;

    /**
     * Returns the next batch of quads.
     * The result can contain more conflict clusters including quads with multiple different subjects.
     * The result MUST guarantee that all quads sharing the same subject are returned in one batch and
     * no other batches will contain quads that share the subject.
     * Returning an empty collection is a valid output, it doesn't indicate end of data as long as
     * {@link #hasNext()} returns true, though.
     * @return next batch of quads
     * @throws ODCSFusionToolException error
     */
    ResourceDescription nextQuads() throws ODCSFusionToolException;

    /**
     * Indicates if there are more quads available.
     * @return true if there are more quads available
     * @throws ODCSFusionToolException error
     */
    boolean hasNext() throws ODCSFusionToolException;

    /**
     * This method can be used to update state of quad loader with result of conflict resolution
     * on quads returned by {@link #nextQuads()}.
     * @param resolvedStatements resolved quads produced by conflict resolver
     */
    void updateWithResolvedStatements(Collection<ResolvedStatement> resolvedStatements);
}
