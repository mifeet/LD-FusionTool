package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import org.openrdf.model.Model;

import java.io.IOException;

public interface FusionToolComponentFactory {
    InputLoader getInputLoader() throws IOException, ODCSFusionToolException;

    CloseableRDFWriter getRDFWriter() throws IOException, ODCSFusionToolException;

    Model getMetadata() throws ODCSFusionToolException;

    UriMappingIterable getUriMapping() throws ODCSFusionToolException, IOException;

    ResourceDescriptionConflictResolver getConflictResolver(Model metadata, UriMappingIterable uriMapping) throws ODCSFusionToolException;

    FusionToolExecutor getExecutor(UriMappingIterable uriMapping) throws ODCSFusionToolException;

    UriMappingWriter getCanonicalUriWriter(UriMappingIterable uriMapping) throws IOException, ODCSFusionToolException;

    UriMappingWriter getSameAsLinksWriter() throws IOException, ODCSFusionToolException;
}
