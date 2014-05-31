package cz.cuni.mff.odcleanstore.fusiontool.io;

import com.google.common.base.Preconditions;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.EnumDataSourceType;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolAppUtils;
import cz.cuni.mff.odcleanstore.fusiontool.util.OutputParamReader;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.util.RDFLoader;
import org.openrdf.rio.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Utility class that parses an RDF file specified by input configuration and passes the data
 * to the given handler.
 */
public class RdfFileLoader {
    private static final Logger LOG = LoggerFactory.getLogger(RdfFileLoader.class);

    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    private final SourceConfig dataSourceConfig;
    private final ParserConfig parserConfig;
    private final OutputParamReader paramReader;

    public RdfFileLoader(SourceConfig sourceConfig, ParserConfig parserConfig) {
        Preconditions.checkNotNull(sourceConfig);
        Preconditions.checkNotNull(parserConfig);
        this.dataSourceConfig = sourceConfig;
        this.paramReader = new OutputParamReader(sourceConfig);
        if (sourceConfig.getType() != EnumDataSourceType.FILE) {
            throw new IllegalArgumentException("The given data source must be of type FILE, " + sourceConfig.getType() + " given");
        }
        this.parserConfig = parserConfig;
    }

    public void read(RDFHandler rdfHandler) throws ODCSFusionToolException, RDFHandlerException {
        String label = dataSourceConfig.getName() != null ? dataSourceConfig.getName() : dataSourceConfig.getType().toString();
        String displayPath = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH, "");
        try {
            loadFile(label, rdfHandler);
        } catch (IOException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.RDF_FILE_LOADER_READ, "I/O Error while reading input file " + displayPath, e);
        } catch (RDFParseException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.RDF_FILE_LOADER_PARSE, "Error parsing input file " + displayPath, e);
        }
    }

    private void loadFile(String label, RDFHandler rdfHandler)
            throws ODCSFusionToolException, IOException, RDFParseException, RDFHandlerException {

        String path = paramReader.getRequiredStringValue(ConfigParameters.DATA_SOURCE_FILE_PATH);
        File file = new File(path);
        String baseURI = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_BASE_URI);
        if (baseURI == null || ODCSUtils.isValidIRI(baseURI)) {
            baseURI = file.toURI().toString();
        }
        String format = paramReader.getStringValue(ConfigParameters.DATA_SOURCE_FILE_FORMAT);
        RDFFormat sesameFormat = ODCSFusionToolAppUtils.getSesameSerializationFormat(format, file.getName());
        if (sesameFormat == null) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_CONFIG,
                    "Unknown serialization format " + format + " for input file " + label);
        }

        RDFLoader loader = new RDFLoader(parserConfig, VF);
        loader.load(file, baseURI, sesameFormat, rdfHandler);
    }
}
