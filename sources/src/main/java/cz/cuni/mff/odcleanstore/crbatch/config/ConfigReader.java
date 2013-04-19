/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

import cz.cuni.mff.odcleanstore.conflictresolution.AggregationSpec;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationType;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.AggregationXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ConfigXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ConflictResolutionXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.DataSourceXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.OutputXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.ParamXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.PrefixXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.PropertyXml;
import cz.cuni.mff.odcleanstore.crbatch.config.xml.RestrictionXml;
import cz.cuni.mff.odcleanstore.crbatch.exceptions.InvalidInputException;
import cz.cuni.mff.odcleanstore.crbatch.io.EnumSerializationFormat;
import cz.cuni.mff.odcleanstore.crbatch.util.CRBatchUtils;
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
        Map<String, String> prefixes;
        if (configXml.getPrefixes() != null) {
            prefixes = extractPrefixes(configXml.getPrefixes());
        } else {
            prefixes = new HashMap<String, String>();
        }
        config.setPrefixes(Collections.unmodifiableMap(prefixes));

        // Data sources
        List<DataSourceConfig> dataSources = new LinkedList<DataSourceConfig>();
        for (DataSourceXml dsXml : configXml.getDataSources()) {
            dataSources.add(extractDataSource(dsXml));
        }
        config.setDataSources(Collections.unmodifiableList(dataSources));
        
        // Data processing settings
        if (configXml.getDataProcessing() != null) {
            RestrictionXml seedRestriction = configXml.getDataProcessing().getSeedResourceRestriction();
            config.setSeedResourceRestriction(extractResourceRestriction(seedRestriction));
            List<ParamXml> params = configXml.getDataProcessing().getParams();
            if (params != null) {
                extractDataProcessingParams(params, config);
            }
        }
        
        // Conflict resolution settings
        if (configXml.getConflictResolution() != null) {
            config.setAggregationSpec(extractAggregationSpec(configXml.getConflictResolution()));
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

    private void extractDataProcessingParams(List<ParamXml> params, ConfigImpl config) throws InvalidInputException {
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
            } else if ("enableFileCache".equalsIgnoreCase(param.getName())) {
                config.setEnableFileCache(Boolean.parseBoolean(param.getValue()));                
            } else if ("maxOutputTriples".equalsIgnoreCase(param.getName())) {
                long value = convertToLong(param.getValue(), "Value of maxOutputTriples is not a valid number");
                config.setMaxOutputTriples(value);
            } else {
                throw new InvalidInputException("Unknown parameter " + param.getName()
                        + " used in conflict resolution parameters");
            }
        }
    }

    private DataSourceConfig extractDataSource(DataSourceXml dataSourceXml) throws InvalidInputException {
        EnumDataSourceType type;
        try {
            type = EnumDataSourceType.valueOf(dataSourceXml.getType().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException("Unknown type of data source: " + dataSourceXml.getType());
        }
        
        DataSourceConfigImpl dataSourceConfig = new DataSourceConfigImpl(type, dataSourceXml.getName());
        
        for (ParamXml param : dataSourceXml.getParams()) {
            dataSourceConfig.getParams().put(param.getName().toLowerCase(), param.getValue());
        }
        
        SparqlRestriction namedGraphResriction = extractGraphRestriction(dataSourceXml.getGraphRestriction());
        if (namedGraphResriction != null) {
            dataSourceConfig.setNamedGraphRestriction(namedGraphResriction);
        }
        dataSourceConfig.setMetadataGraphRestriction(extractGraphRestriction(dataSourceXml.getMetadataGraphRestriction()));

        return dataSourceConfig;
    }

    private Output extractOutput(OutputXml outputXml) throws InvalidInputException {
        String formatString = extractParamByName(outputXml.getParams(), "format");
        EnumSerializationFormat format = EnumSerializationFormat.parseFormat(formatString);
        if (formatString == null) {
            throw new InvalidInputException("Output format must be specified");
        } else if (format == null) {
            throw new InvalidInputException("Unknown output format " + formatString);
        }

        String fileLocationString = extractParamByName(outputXml.getParams(), "file");
        if (fileLocationString == null) {
            throw new InvalidInputException("Name of the output file must be specified");
        }
        File fileLocation = new File(fileLocationString);
        
        // Create result object
        OutputImpl output = new OutputImpl(format, fileLocation);
        
        // Add optional parameters
        String sameAsLocationString = extractParamByName(outputXml.getParams(), "sameAsFile");
        if (sameAsLocationString != null) {
            output.setSameAsFileLocation(new File(sameAsLocationString));
        }

        String splitByMB = extractParamByName(outputXml.getParams(), "splitByMB");
        if (splitByMB != null) {
            final String errorMessage = "Value of splitByMB for output is not a valid number";
            long value = convertToLong(splitByMB, errorMessage);
            if (value <= 0) {
                throw new InvalidInputException(errorMessage);
            }
            output.setSplitByBytes(value * CRBatchUtils.MB_BYTES);
        }
        
        String metadataContext = extractParamByName(outputXml.getParams(), "metadataContext");
        if (metadataContext != null) {
            URI uri = convertToURI(metadataContext, "metadataContext is not a valid URI");
            output.setMetadataContext(uri);
        }

        return output;
    }

    private long convertToLong(String str, String errorMessage) throws InvalidInputException {
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            throw new InvalidInputException(errorMessage, e);
        }
    }
    
    private URI convertToURI(String str, String errorMessage) throws InvalidInputException {
        try {
            return ValueFactoryImpl.getInstance().createURI(str);
        } catch (IllegalArgumentException e) {
            throw new InvalidInputException(errorMessage, e);
        }
    }

    private SparqlRestriction extractGraphRestriction(RestrictionXml restrictionXml) {
        return extractRestriction(restrictionXml, ConfigConstants.DEFAULT_RESTRICTION_GRAPH_VAR);
    }
    
    private SparqlRestriction extractResourceRestriction(RestrictionXml restrictionXml) {
        return extractRestriction(restrictionXml, ConfigConstants.DEFAULT_RESTRICTION_RESOURCE_VAR);
    }
    
    private SparqlRestriction extractRestriction(RestrictionXml restrictionXml, String defaultVarName) {
        if (restrictionXml == null) {
            return null;
        }
        String pattern = preprocessGroupGraphPattern(restrictionXml.getValue());
        
        //if (pattern.isEmpty()) {
        //    return null;
        //}
        if (restrictionXml.getVar() == null) {
            return new SparqlRestrictionImpl(pattern, defaultVarName);
        } else {
            return new SparqlRestrictionImpl(pattern, restrictionXml.getVar());
        }
    }

    /**
     * Prepares SPARQL group graph pattern - trims whitespace and optional enclosing braces.
     * @param groupGraphPattern SPARQL group graph pattern
     * @return group graph pattern with trimmed whitespace & enclosing braces
     */
    private String preprocessGroupGraphPattern(String groupGraphPattern) {
        String result = groupGraphPattern.trim();
        return result;
    }

    private ConfigReader() {
    }
}
