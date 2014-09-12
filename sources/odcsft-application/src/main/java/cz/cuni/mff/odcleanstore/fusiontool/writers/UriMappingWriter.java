package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;

import java.io.IOException;

public interface UriMappingWriter {
    void write(UriMappingIterable uriMapping) throws IOException;
}
