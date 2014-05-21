package cz.cuni.mff.odcleanstore.fusiontool.config;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolutionStrategy;
import org.openrdf.model.URI;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Configuration related to conflict resolution.
 */
public interface ConfigConflictResolution {
    /**
     * Default conflict resolution strategy.
     * @return resolution strategy
     */
    ResolutionStrategy getDefaultResolutionStrategy();

    /**
     * Conflicts resolution strategy settings for individual properties.
     * Key is the property URI (must have expanded namespace prefix), value the actual strategy.
     * @return map of resolution strategies indexed by property URIs
     */
    Map<URI, ResolutionStrategy> getPropertyResolutionStrategies();

    /**
     * Returns a default set of preferred URIs.
     * These are added to preferred URIs obtained from configuration and canonical URI file.
     * @return set of preferred canonical URIs
     */
    Collection<String> getPreferredCanonicalURIs();

    /**
     * Returns set of URIs that act as an owl:sameAs link or it's equivalent for the purposes of conflict resolution.
     * @return set of 'same as' link property URIs
     */
    Set<URI> getSameAsLinkTypes();
}
