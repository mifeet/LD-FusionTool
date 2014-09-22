package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.config.Output;
import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.source.Source;

import java.util.Map;

/**
 * Helper class for parsing data source parameters.
 */
public class OutputParamReader extends ParamReader {
    private OutputParamReader(Map<String, String> params, String ioName, String ioType) {
        super(params, String.format("%s '%s'", ioType, ioName));
    }

    public OutputParamReader(Output output) {
        this(output.getParams(),
                output.getName() != null ? output.getName() : output.getType().name(),
                "output");
    }

    public OutputParamReader(SourceConfig sourceConfig) {
        this(sourceConfig.getParams(),
                sourceConfig.getName() != null ? sourceConfig.getName() : sourceConfig.getType().name(),
                "input");
    }

    public OutputParamReader(Source source) {
        this(source.getParams(),
                source.getName() != null ? source.getName() : source.getType().name(),
                "input");
    }
}
