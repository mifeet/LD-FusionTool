package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.ParamReader;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Loader of all triples from the given file data source to the given RDF handler.
 */
public class AllTriplesFileLoader implements AllTriplesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AllTriplesFileLoader.class);

    private static final ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();

    private final DataSourceConfig dataSourceConfig;
    private final ParserConfig parserConfig;
    private final ParamReader paramReader;

    public AllTriplesFileLoader(DataSourceConfig dataSourceConfig, ParserConfig parserConfig) {
        ODCSFusionToolUtils.checkNotNull(dataSourceConfig);
        ODCSFusionToolUtils.checkNotNull(parserConfig);
        this.dataSourceConfig = dataSourceConfig;
        this.paramReader = new ParamReader(dataSourceConfig);
        if (dataSourceConfig.getType() != EnumDataSourceType.FILE) {
            throw new IllegalArgumentException("The given data source must be of type FILE, " + dataSourceConfig.getType() + " given");
        }
        this.parserConfig = parserConfig;
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws ODCSFusionToolException {
        String sourceLabel = dataSourceConfig.getName() != null ? dataSourceConfig.getName() : dataSourceConfig.getType().toString();
        LOG.info("Parsing all quads from data source {}", sourceLabel);

        String displayPath = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH, "");
        try {
            parseFileDataSource(sourceLabel, rdfHandler);
        } catch (IOException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_READ_FILE, "I/O Error while reading input file " + displayPath, e);
        } catch (RDFParseException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_PARSE_FILE, "Error parsing input file " + displayPath, e);
        } catch (RDFHandlerException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.INPUT_LOADER_BUFFER_QUADS, "Error processing quads from input " + sourceLabel, e);
        }
    }

    @Override
    public URI getDefaultContext() throws ODCSFusionToolException {
        String baseURI = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (baseURI == null || !ODCSUtils.isValidIRI(baseURI)) {
            String path = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH);
            File file = new File(path);
            baseURI = file.toURI().toString();
        }
        return VALUE_FACTORY.createURI(baseURI);
    }

    private void parseFileDataSource(String sourceLabel, RDFHandler inputLoaderPreprocessor)
            throws ODCSFusionToolException, IOException, RDFParseException, RDFHandlerException {

        String path = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH);
        File file = new File(path);
        String baseURI = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (baseURI == null || ODCSUtils.isValidIRI(baseURI)) {
            baseURI = file.toURI().toString();
        }
        String format = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_FORMAT);
        RDFFormat sesameFormat = ODCSFusionToolUtils.getSesameSerializationFormat(format, file.getName());
        if (sesameFormat == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Unknown serialization format " + format + " for data source " + sourceLabel);
        }

        RDFLoader loader = new RDFLoader(parserConfig, VALUE_FACTORY);
        loader.load(file, baseURI, sesameFormat, inputLoaderPreprocessor);
    }

    @Override
    public void close() throws ODCSFusionToolException {
        // do nothing
    }
}
