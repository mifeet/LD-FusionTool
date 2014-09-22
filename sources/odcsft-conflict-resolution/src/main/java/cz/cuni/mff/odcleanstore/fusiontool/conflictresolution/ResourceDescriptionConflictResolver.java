package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.conflictresolution.exceptions.ConflictResolutionException;

import java.util.Collection;

/**
 * RDF conflict resolver.
 * Applies conflict resolution to the given resource description of a single resource and
 * returns the resolved quads together with provenance and quality information wrapped
 * in {@link cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement}.
 */
public interface ResourceDescriptionConflictResolver {
    /**
     * Apply conflict resolution process to the given resource description and return result
     * @param resourceDescription container of quads that make up the description of the respective statement
     *      (i.e. quads that are relevant for the conflict resolution process)
     * @return collection of quads derived from the input with resolved
     *         conflicts, (F-)quality estimate and provenance information.
     * @throws ConflictResolutionException error during the conflict resolution process 
     * @see ResolvedStatement
     */
    Collection<ResolvedStatement> resolveConflicts(ResourceDescription resourceDescription) throws ConflictResolutionException;
}