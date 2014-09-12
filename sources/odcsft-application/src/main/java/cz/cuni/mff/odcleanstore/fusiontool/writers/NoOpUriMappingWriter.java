package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;

import java.io.IOException;

public class NoOpUriMappingWriter implements UriMappingWriter {
    @Override
    public void write(UriMappingIterable uriMapping) throws IOException {
        // do nothing
    }
}
