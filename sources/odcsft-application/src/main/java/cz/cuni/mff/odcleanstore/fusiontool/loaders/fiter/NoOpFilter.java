package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;

public class NoOpFilter implements ResourceDescriptionFilter {
    @Override
    public boolean accept(ResourceDescription resourceDescription) {
        return true;
    }
}
