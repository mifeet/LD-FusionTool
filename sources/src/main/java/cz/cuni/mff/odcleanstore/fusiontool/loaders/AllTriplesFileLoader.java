package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Loader of all triples from the given file data source to the given RDF handler.
 */
public class AllTriplesFileLoader implements AllTriplesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AllTriplesFileLoader.class);

    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

    private DataSourceConfig dataSourceConfig;

    public AllTriplesFileLoader(DataSourceConfig dataSourceConfig) {
        this.dataSourceConfig = dataSourceConfig;
        if (dataSourceConfig.getType() != EnumDataSourceType.FILE) {
            throw new IllegalArgumentException("The given data source must be of type FILE, " + dataSourceConfig.getType() + " given");
        }
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws ODCSFusionToolException {
        String sourceLabel = dataSourceConfig.getName() != null ? dataSourceConfig.getName() : dataSourceConfig.getType().toString();
        LOG.info("Parsing all quads from data source {}", sourceLabel);

        String displayPath = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH);
        displayPath = displayPath == null ? "" : displayPath;
        try {
            parseFileDataSource(rdfHandler);
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_READ_FILE, "I/O Error while reading input file " + displayPath, e);
        } catch (RDFParseException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_PARSE_FILE, "Error parsing input file " + displayPath, e);
        } catch (RDFHandlerException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_BUFFER_QUADS, "Error processing quads from input " + sourceLabel, e);
        }
    }

    @Override
    public URI getDefaultContext() {
        String uri = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (uri != null && ODCSUtils.isValidIRI(uri)) {
            return VALUE_FACTORY.createURI(uri);
        }
        String path = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH);
        if (path != null) {
            return VALUE_FACTORY.createURI(new File(path).toURI().toString());
        }
        return null;
    }

    private void parseFileDataSource(RDFHandler inputLoaderPreprocessor)
            throws ODCSFusionToolException, IOException, RDFParseException, RDFHandlerException {
        String sourceLabel = dataSourceConfig.getName() != null ? dataSourceConfig.getName() : dataSourceConfig.getType().toString();

        String path = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_PATH);
        if (path == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG, "Missing required parameter path for data source " + sourceLabel);
        }
        File inputFile = new File(path);

        String baseURI = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (baseURI == null) {
            baseURI = inputFile.toURI().toString();
        }

        String format = dataSourceConfig.getParams().get(ConfigParameters.DATA_SOURCE_FILE_FORMAT);
        RDFFormat sesameFormat = ODCSFusionToolUtils.getSesameSerializationFormat(format, inputFile.getName());
        if (sesameFormat == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Unknown serialization format " + format + " for data source " + sourceLabel);
        }

        RDFParser rdfParser = Rio.createParser(sesameFormat, VALUE_FACTORY);
        rdfParser.setRDFHandler(inputLoaderPreprocessor);
        rdfParser.parse(new FileInputStream(inputFile), baseURI);
    }

    @Override
    public void close() throws ODCSFusionToolException {
        // do nothing
    }
}
