package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;

import java.util.List;

public class FederatedResourceDescriptionFilter implements ResourceDescriptionFilter {
    private final ResourceDescriptionFilter[] filters;

    public static ResourceDescriptionFilter fromList(List<ResourceDescriptionFilter> inputFilters) {
        switch (inputFilters.size()) {
        case 0:
            return new NoOpFilter();
        case 1:
            return inputFilters.get(0);
        default:
            return new FederatedResourceDescriptionFilter(inputFilters.toArray(new ResourceDescriptionFilter[inputFilters.size()]));
        }
    }

    protected FederatedResourceDescriptionFilter(ResourceDescriptionFilter... filters) {
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
