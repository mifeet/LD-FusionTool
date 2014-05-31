package cz.cuni.mff.odcleanstore.fusiontool.loaders.data;

import com.google.common.base.Preconditions;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.config.ConfigParameters;
import cz.cuni.mff.odcleanstore.fusiontool.config.DataSourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.RdfFileLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.OutputParamReader;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Loader of all triples from the given file data source to the given RDF handler.
 */
public class AllTriplesFileLoader implements AllTriplesLoader {
    private static final Logger LOG = LoggerFactory.getLogger(AllTriplesFileLoader.class);
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    private final OutputParamReader paramReader;
    private final RdfFileLoader fileLoader;

    public AllTriplesFileLoader(DataSourceConfig dataSourceConfig, ParserConfig parserConfig) {
        Preconditions.checkNotNull(dataSourceConfig);
        Preconditions.checkNotNull(parserConfig);
        this.fileLoader = new RdfFileLoader(dataSourceConfig, parserConfig);
        this.paramReader = new OutputParamReader(dataSourceConfig);
    }

    @Override
    public void loadAllTriples(RDFHandler rdfHandler) throws ODCSFusionToolException {
        LOG.info("Parsing all quads from {}", paramReader.getLabel());
        try {
            this.fileLoader.read(rdfHandler);
        } catch (RDFHandlerException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.INPUT_LOADER_BUFFER_QUADS, "Error processing quads from " + paramReader.getLabel(), e);
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
        return VF.createURI(baseURI);
    }

    @Override
    public void close() throws ODCSFusionToolException {
        // do nothing
    }
}
