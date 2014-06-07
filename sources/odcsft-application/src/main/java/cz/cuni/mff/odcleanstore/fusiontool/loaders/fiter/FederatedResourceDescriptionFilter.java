package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;

public class FederatedResourceDescriptionFilter implements ResourceDescriptionFilter {
    private final ResourceDescriptionFilter[] filters;

    public FederatedResourceDescriptionFilter(ResourceDescriptionFilter... filters) {
        this.filters = filters;
    }

    @Override
    public boolean accept(ResourceDescription resourceDescription) {
        for (ResourceDescriptionFilter filter : filters) {
            if (!filter.accept(resourceDescription)) {
                return false;
            }
        }
        return true;
    }
}
