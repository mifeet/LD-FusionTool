package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import cz.cuni.mff.odcleanstore.fusiontool.util.CanonicalUriFileHelper;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CanonicalUriFileWriter implements UriMappingWriter {

    public static final CanonicalUriFileHelper CANONICAL_URI_FILE_HELPER = new CanonicalUriFileHelper();

    private final File outputFile;

    public CanonicalUriFileWriter(File outputFile) {
        this.outputFile = outputFile;
    }

    @Override
    public void write(UriMappingIterable uriMapping) throws IOException {
        Set<String> canonicalUris = new HashSet<>();
        for (String mappedUri : uriMapping) {
            canonicalUris.add(uriMapping.getCanonicalURI(mappedUri));
        }
        CANONICAL_URI_FILE_HELPER.writeCanonicalUris(outputFile, canonicalUris);
    }
}
