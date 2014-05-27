package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Collection;

/**
 * Representation of a resource description consisting of the name of the resource and
 * {@link org.openrdf.model.Statement}s that describe this resource.
 * What statements are included in the description is dependent on the implementation and usage
 * (e.g. CBD).
 */
public interface ResourceDescription {
    /**
     * The resource being described.
     * @return resource
     */
    Resource getResource();

    /**
     * Quads describing the resource returned by {@link #getResource()}.
     * @return collection of statements describing the resource
     */
    Collection<Statement> getDescribingStatements();
}
