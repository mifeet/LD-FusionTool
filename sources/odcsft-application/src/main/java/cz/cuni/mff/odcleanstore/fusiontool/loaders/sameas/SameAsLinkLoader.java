package cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;

/**
 * Loader of owl:sameAs (or equivalent) links.
 */
public interface SameAsLinkLoader {
    /**
     * Loads owl:sameAs (or equivalent) links from the underlying data source and adds them to the given canonical URI mapping.
     * @param uriMapping URI mapping where loaded links will be added
     * @return number of loaded owl:sameAs links
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolExceptionException repository error
     */
    public long loadSameAsMappings(UriMappingImpl uriMapping) throws LDFusionToolException;
}
