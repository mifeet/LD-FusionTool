package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConstants;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import java.io.File;
import java.io.IOException;

public class FusionToolRdfLoader {
    private final RDFLoader rdfLoader;

    public FusionToolRdfLoader() {
        rdfLoader = new RDFLoader(ConfigConstants.DEFAULT_FILE_PARSER_CONFIG, ValueFactoryImpl.getInstance());
    }

    /**
     * Parses RDF data from the specified file to the given RDFHandler.
     * @param file A file containing RDF data.
     * @param baseURI The base URI to resolve any relative URIs that are in the data
     * against. This defaults to the value of {@link java.io.File#toURI()
     * file.toURI()} if the value is set to <tt>null</tt>.
     * @param dataFormat The serialization format of the data.
     * @param rdfHandler Receives RDF parser events.
     * @throws IOException If an I/O error occurred while reading from the file.
     * @throws org.openrdf.rio.UnsupportedRDFormatException If no parser is available for the specified RDF format.
     * @throws RDFParseException If an error was found while parsing the RDF data.
     * @throws RDFHandlerException If thrown by the RDFHandler
     */
    public void load(File file, String baseURI, RDFFormat dataFormat, RDFHandler rdfHandler)
            throws IOException, RDFParseException, RDFHandlerException {
        rdfLoader.load(file, baseURI, dataFormat, rdfHandler);
    }
}
