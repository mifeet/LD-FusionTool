package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.AlternativeUriNavigator;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class MappedResourceFilter implements ResourceDescriptionFilter {
    private AlternativeUriNavigator alternativeUriNavigator;

    public MappedResourceFilter(AlternativeUriNavigator alternativeUriNavigator) {
        this.alternativeUriNavigator = alternativeUriNavigator;
    }

    @Override
    public boolean accept(ResourceDescription resourceDescription) {
        Resource resource = resourceDescription.getResource();
        if ((resource instanceof URI) && alternativeUriNavigator.hasAlternativeUris((URI) resource)) {
            return true;
        }
        return false; // skip statement whose resource has no alternative URIs
    }
}
