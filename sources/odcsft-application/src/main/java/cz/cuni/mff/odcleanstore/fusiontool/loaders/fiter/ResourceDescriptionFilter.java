package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;

/**
 * Filter of resource descriptions loaded from input data.
 */
public interface ResourceDescriptionFilter {
    boolean accept(ResourceDescription resourceDescription);
}
