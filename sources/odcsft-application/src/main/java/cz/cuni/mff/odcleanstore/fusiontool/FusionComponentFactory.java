package cz.cuni.mff.odcleanstore.fusiontool;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.ResourceDescriptionConflictResolver;
import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.loaders.InputLoader;
import cz.cuni.mff.odcleanstore.fusiontool.writers.CloseableRDFWriter;
import cz.cuni.mff.odcleanstore.fusiontool.writers.UriMappingWriter;
import org.openrdf.model.Model;

import java.io.IOException;

public interface FusionComponentFactory {
    InputLoader getInputLoader() throws IOException, LDFusionToolException;

    CloseableRDFWriter getRDFWriter() throws IOException, LDFusionToolException;

    Model getMetadata() throws LDFusionToolException;

    UriMappingIterable getUriMapping() throws LDFusionToolException, IOException;

    ResourceDescriptionConflictResolver getConflictResolver(Model metadata, UriMappingIterable uriMapping) throws LDFusionToolException;

    FusionExecutor getExecutor(UriMappingIterable uriMapping) throws LDFusionToolException;

    UriMappingWriter getCanonicalUriWriter(UriMappingIterable uriMapping) throws IOException, LDFusionToolException;

    UriMappingWriter getSameAsLinksWriter() throws IOException, LDFusionToolException;
}
