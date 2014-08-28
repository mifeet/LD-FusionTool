package cz.cuni.mff.odcleanstore.fusiontool.config;

import org.openrdf.model.URI;
import org.openrdf.rio.ParserConfig;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Configuration related to input and output.
 */
public interface ConfigIO {
    /**
     * List of data sources.
     * @return list of data sources.
     */
    List<DataSourceConfig> getDataSources();

    /**
     * List of owl:sameAs links sources.
     * @return list of data sources.
     */
    List<ConstructSourceConfig> getSameAsSources();

    /**
     * List of metadata sources.
     * @return list of data sources.
     */
    List<ConstructSourceConfig> getMetadataSources();

    /**
     * List of result data outputs.
     * @return list of result data outputs
     */
    List<Output> getOutputs();

    /**
     * File where resolved canonical URIs shall be written.
     * Null means that canonical URIs will not be written anywhere.
     * @return file to write canonical URIs to or null
     */
    File getCanonicalURIsOutputFile();

    /**
     * File with list of preferred canonical URIs, one URI per line.
     * Null means no preferred URIs.
     * @return file with canonical URIs
     */
    File getCanonicalURIsInputFile();

    /**
     * Directory for temporary files.
     * @return directory for temporary files.
     */
    File getTempDirectory();
}
