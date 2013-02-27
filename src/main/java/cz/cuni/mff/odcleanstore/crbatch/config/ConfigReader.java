/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationType;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.AggregationXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ConfigXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ConflictResolutionXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.OutputXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ParamXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.PrefixXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.PropertyXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.SourceGraphsRestrictionXml;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.crbatch.io.EnumOutputFormat;
import cz.cuni.mff.odcleanstore.crbatch.util.NamespacePrefixExpander;
import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * Reads the XML configuration file and produces instances of configuration in a {@link Config} instance.
 * @author Jan Michelfeit
 */
public final class ConfigReader {

    /**
     * Parses the given configuration file and produces returns the contained configuration as an {@link Config} instance.
     * @param configFile configuration XMl file
     * @return parsed configuration
     * @throws InvalidInputException parsing error
     */
    public static Config parseConfigXml(File configFile) throws InvalidInputException {
        ConfigReader instance = new ConfigReader();
        return instance.parseConfigXmlImpl(configFile);
    }

    private Config parseConfigXmlImpl(File configFile) throws InvalidInputException {
        ConfigImpl config = new ConfigImpl();

        Serializer serializer = new Persister();
        ConfigXml configXml;
        try {
            configXml = serializer.read(ConfigXml.class, configFile);
        } catch (Exception e) {
            throw new InvalidInputException("Error parsing configuration file", e);
        }

        // Prefixes
        if (configXml.getPrefixes() != null) {
            config.setPrefixes(extractPrefixes(configXml.getPrefixes()));
        } else {
            config.setPrefixes(new HashMap<String, String>());
        }

        // Data source
        List<ParamXml> dataSourceParams = configXml.getDataSource();
        config.setDatabaseConnectionString(extractDatabaseConnectionString(dataSourceParams));
        config.setDatabaseUsername(extractParamByName(dataSourceParams, "username"));
        config.setDatabasePassword(extractParamByName(dataSourceParams, "password"));

        // Source graphs restriction
        if (configXml.getSourceGraphsRestriction() != null) {
            SourceGraphsRestrictionXml restriction = configXml.getSourceGraphsRestriction();
            if (restriction.getGraphvar() != null) {
                config.setNamedGraphRestrictionVar(restriction.getGraphvar());
            }
            config.setNamedGraphRestrictionPattern(preprocessGroupGraphPattern(restriction.getValue()));
        }

        // Conflict resolution settings
        if (configXml.getConflictResolution() != null) {
            config.setAggregationSpec(extractAggregationSpec(configXml.getConflictResolution()));
            if (configXml.getConflictResolution().getParams() != null) {
                List<ParamXml> params = configXml.getConflictResolution().getParams();
                extractConflictResolutionParams(params, config);
            }
        } else {
            config.setAggregationSpec(new AggregationSpec());
        }

        // Outputs
        List<Output> outputs = new LinkedList<Output>();
        for (OutputXml outputXml : configXml.getOutputs()) {
            outputs.add(extractOutput(outputXml));
        }
        config.setOutputs(outputs);
        
        // Expand prefixes in CR settings
        NamespacePrefixExpander prefixExpander = new NamespacePrefixExpander(config.getPrefixes());
        config.setAggregationSpec(prefixExpander.expandPropertyNames(config.getAggregationSpec()));

        return config;
    }

    private String extractParamByName(List<ParamXml> params, String name) {
        for (ParamXml param : params) {
            if (name.equalsIgnoreCase(param.getName())) {
                return param.getValue();
            }
        }
        return null;
    }

    private Map<String, String> extractPrefixes(List<PrefixXml> prefixes) {
        Map<String, String> prefixMap = new HashMap<String, String>();
        for (PrefixXml prefixXml : prefixes) {
            prefixMap.put(prefixXml.getId(), prefixXml.getNamespace());
        }
        return prefixMap;
    }

    private String extractDatabaseConnectionString(List<ParamXml> dataSource) throws InvalidInputException {
        String host = extractParamByName(dataSource, "host");
        String port = extractParamByName(dataSource, "port");
        if (host == null) {
            throw new InvalidInputException("Database host must be specified in <DataSource>");
        }
        if (port == null) {
            throw new InvalidInputException("Database connection port must be specified in <DataSource>");
        }

        return "jdbc:virtuoso://" + host + ":" + port + "/CHARSET=UTF-8";
    }

    private AggregationSpec extractAggregationSpec(ConflictResolutionXml conflictResolutionXml) throws InvalidInputException {
        AggregationSpec aggregationSpec = new AggregationSpec();
        if (conflictResolutionXml.getDefaultAggregation() != null) {
            extractAggregationDefaultSettings(conflictResolutionXml.getDefaultAggregation(), aggregationSpec);
        }
        if (conflictResolutionXml.getPropertyAggregations() != null) {
            for (AggregationXml aggregationSetting : conflictResolutionXml.getPropertyAggregations()) {
                extractAggregationPropertySettings(aggregationSetting, aggregationSpec);
            }
        }
        return aggregationSpec;
    }

    private void extractAggregationDefaultSettings(List<ParamXml> defaultSettings, AggregationSpec aggregationSpec)
            throws InvalidInputException {

        for (ParamXml defaultParam : defaultSettings) {
            if ("defaultMultivalue".equalsIgnoreCase(defaultParam.getName())) {
                boolean value = Boolean.parseBoolean(defaultParam.getValue());
                aggregationSpec.setDefaultMultivalue(value);
            } else if ("defaultAggregation".equalsIgnoreCase(defaultParam.getName())) {
                try {
                    EnumAggregationType value = EnumAggregationType.valueOf(defaultParam.getValue());
                    aggregationSpec.setDefaultAggregation(value);
                } catch (Exception e) {
                    throw new InvalidInputException("Invalid value " + defaultParam.getValue()
                            + " for default aggregation method", e);
                }
            } else if ("aggregationErrorStrategy".equalsIgnoreCase(defaultParam.getName())) {
                try {
                    EnumAggregationErrorStrategy value = EnumAggregationErrorStrategy.valueOf(defaultParam.getValue());
                    aggregationSpec.setErrorStrategy(value);
                } catch (Exception e) {
                    throw new InvalidInputException("Invalid value " + defaultParam.getValue()
                            + " for aggregation error strategy", e);
                }
            } else {
                throw new InvalidInputException("Unknown parameter " + defaultParam.getName()
                        + " used as default conflict resoulution setting");
            }
        }
    }

    private void extractAggregationPropertySettings(AggregationXml aggregationSetting, AggregationSpec aggregationSpec) {
        if (aggregationSetting.getType() != null) {
            for (PropertyXml property : aggregationSetting.getProperties()) {
                aggregationSpec.getPropertyAggregations().put(property.getId(), aggregationSetting.getType());
            }
        }
        if (aggregationSetting.getMultivalue() != null) {
            for (PropertyXml property : aggregationSetting.getProperties()) {
                aggregationSpec.getPropertyMultivalue().put(property.getId(), aggregationSetting.getMultivalue());
            }
        }
    }

    private void extractConflictResolutionParams(List<ParamXml> params, ConfigImpl config) throws InvalidInputException {
        for (ParamXml param : params) {
            if (param.getValue() == null) {
                continue;
            }
            if ("canonicalUriOutputFile".equalsIgnoreCase(param.getName())) {
                if (!ODCSUtils.isNullOrEmpty(param.getValue())) {
                    config.setCanonicalURIsOutputFile(new File(param.getValue()));
                } else {
                    config.setCanonicalURIsOutputFile(null);
                }
            } else if ("canonicalUriInputFile".equalsIgnoreCase(param.getName())) {
                if (!ODCSUtils.isNullOrEmpty(param.getValue())) {
                    config.setCanonicalURIsInputFile(new File(param.getValue()));
                } else {
                    config.setCanonicalURIsInputFile(null);
                }
            } else {
                throw new InvalidInputException("Unknown parameter " + param.getName()
                        + " used in conflict resolution parameters");
            }
        }
    }

    private Output extractOutput(OutputXml outputXml) throws InvalidInputException {
        String formatString = extractParamByName(outputXml.getParams(), "format");
        EnumOutputFormat format;
        if ("ntriples".equalsIgnoreCase(formatString) || "n3".equalsIgnoreCase(formatString)) {
            format = EnumOutputFormat.N3;
        } else if ("rdf/xml".equalsIgnoreCase(formatString) || "rdfxml".equalsIgnoreCase(formatString)) {
            format = EnumOutputFormat.RDF_XML;
        } else if (formatString == null) {
            throw new InvalidInputException("Output format must be specified");
        } else {
            throw new InvalidInputException("Unknown output format " + formatString);
        }

        String fileLocationString = extractParamByName(outputXml.getParams(), "file");
        if (fileLocationString == null) {
            throw new InvalidInputException("Name of the output file must be specified");
        }
        File fileLocation = new File(fileLocationString);

        return new OutputImpl(format, fileLocation);
    }

    /**
     * Prepares SPARQL group graph pattern - trims whitespace and optional enclosing braces.
     * @param groupGraphPattern SPARQL group graph pattern
     * @return group graph pattern with trimmed whitespace & enclosing braces
     */
    private String preprocessGroupGraphPattern(String groupGraphPattern) {
        String result = groupGraphPattern.trim();
        // if (result.startsWith("{") && result.endsWith("}")) {
        // result = result.substring(1, result.length() - 1);
        // }
        return result;
    }

    private ConfigReader() {
    }
}
