package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import java.util.Collection;

/**
 * Simple immutable implementation of {@link cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescription}.
 */
public class ResourceDescriptionImpl implements ResourceDescription {
    private final Resource resource;
    private final Collection<Statement> describingStatements;

    public ResourceDescriptionImpl(Resource resource, Collection<Statement> describingStatements) {

        this.resource = resource;
        this.describingStatements = describingStatements;
    }

    @Override
    public Resource getResource() {
        return resource;
    }

    @Override
    public Collection<Statement> getDescribingStatements() {
        return describingStatements;
    }
}
