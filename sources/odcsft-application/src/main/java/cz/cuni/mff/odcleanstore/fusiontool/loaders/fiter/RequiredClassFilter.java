package cz.cuni.mff.odcleanstore.fusiontool.loaders.fiter;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.RDF;

public class RequiredClassFilter implements ResourceDescriptionFilter {
    private final URI typeProperty;
    private final UriMapping uriMapping;
    private final Resource requiredClass;

    public RequiredClassFilter(UriMapping uriMapping, URI requiredClass) {
        this.uriMapping = uriMapping;
        this.requiredClass = uriMapping.mapResource(requiredClass);
        this.typeProperty = (URI) uriMapping.mapResource(RDF.TYPE);
    }

    @Override
    public boolean accept(ResourceDescription resourceDescription) {
        Resource canonicalResource = uriMapping.mapResource(resourceDescription.getResource());
        for (Statement statement : resourceDescription.getDescribingStatements()) {
            if (statement.getObject() instanceof Resource
                    && typeProperty.equals(uriMapping.mapResource(statement.getPredicate()))
                    && canonicalResource.equals(uriMapping.mapResource(statement.getSubject()))
                    && requiredClass.equals(uriMapping.mapResource((Resource) statement.getObject()))) {
                return true;
            }
        }
        return false;
    }
}
