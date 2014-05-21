package cz.cuni.mff.odcleanstore.fusiontool.loaders.sameas;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.URIMappingImpl;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.io.RdfFileLoader;
import cz.cuni.mff.odcleanstore.fusiontool.util.ParamReader;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils.checkNotNull;

/**
 * Loader of sameAs links from a file source.
 * Ignores construct pattern but loads all links of type matching
 * {@link cz.cuni.mff.odcleanstore.fusiontool.config.ConfigConflictResolution#getSameAsLinkTypes()}.
 * TODO test
 */
public class SameAsLinkFileLoader implements SameAsLinkLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SameAsLinkFileLoader.class);
    private final ParamReader paramReader;
    private final RdfFileLoader fileLoader;
    private final Set<URI> sameAsLinkTypes;

    public SameAsLinkFileLoader(SourceConfig sourceConfig, ParserConfig parserConfig, Set<URI> sameAsLinkTypes) {
        checkNotNull(sourceConfig);
        checkNotNull(parserConfig);
        checkNotNull(sameAsLinkTypes);
        this.fileLoader = new RdfFileLoader(sourceConfig, parserConfig);
        this.paramReader = new ParamReader(sourceConfig);
        this.sameAsLinkTypes = sameAsLinkTypes;
    }

    @Override
    public long loadSameAsMappings(URIMappingImpl uriMapping) throws ODCSFusionToolException {
        LOG.info("Parsing sameAs links from data source {}", paramReader.getLabel());
        try {
            long startTime = System.currentTimeMillis();
            SameAsLinkHandler linkHandler = new SameAsLinkHandler(uriMapping);
            fileLoader.read(linkHandler);
            LOG.debug(String.format("Loaded & resolved %,d sameAs links from source %s in %,d ms",
                    linkHandler.getLoadedCount(), paramReader.getLabel(), System.currentTimeMillis() - startTime));
            return linkHandler.getLoadedCount();
        } catch (RDFHandlerException e) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.SAME_AS_LOAD, "Error processing sameAs links from input " + paramReader.getLabel(), e);
        }
    }

    private class SameAsLinkHandler extends RDFHandlerBase {
        private final URIMappingImpl uriMapping;
        private long loadedCount = 0;

        public SameAsLinkHandler(URIMappingImpl uriMapping) {
            this.uriMapping = uriMapping;
        }

        @Override
        public void handleStatement(Statement statement) throws RDFHandlerException {
            // Ignore sameAs links between everything but URI resources; see owl:sameAs syntax
            // at http://www.w3.org/TR/2004/REC-owl-semantics-20040210/syntax.html
            if (sameAsLinkTypes.contains(statement.getPredicate())
                    && statement.getObject() instanceof URI
                    && statement.getSubject() instanceof URI) {
                uriMapping.addLink(statement.getSubject().stringValue(), statement.getObject().stringValue());
                loadedCount++;

                if (loadedCount % 500000 == 0) {
                    LOG.info("... loaded {} sameAs links from data source {}", loadedCount, paramReader.getLabel());
                }
            }
        }

        public long getLoadedCount() {
            return loadedCount;
        }
    }
}
