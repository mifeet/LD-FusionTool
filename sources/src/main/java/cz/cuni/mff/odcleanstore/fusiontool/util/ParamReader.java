package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.config.SourceConfig;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

/**
 * Helper class for parsing data source parameters.
 */
public class ParamReader {
    private static final Logger LOG = LoggerFactory.getLogger(ParamReader.class);
    private Map<String, String> params;
    private String sourceName;

    public ParamReader(SourceConfig sourceConfig) {
        this.params = sourceConfig.getParams();
        this.sourceName = sourceConfig.getName() != null ? sourceConfig.getName() : sourceConfig.getType().name();
    }

    public ParamReader(Source source) {
        this.params = source.getParams();
        this.sourceName = source.getName() != null ? source.getName() : source.getType().name();
    }

    public String getRequiredStringValue(String paramName) throws ODCSFusionToolException {
        return getRequiredValue(paramName);
    }

    public String getStringValue(String paramName) {
        return params.get(paramName);
    }

    public String getStringValue(String paramName, String defaultValue) {
        String value = params.get(paramName);
        return value != null ? value : defaultValue;
    }

    public Integer getRequiredIntValue(String paramName) throws ODCSFusionToolException {
        String value = getRequiredValue(paramName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            String message = getNumberFormatErrorMessage(paramName, value);
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.MISSING_REQUIRED_PARAM, message);
        }
    }

    public Integer getIntValue(String paramName) {
        String value = params.get(paramName);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            String message = getNumberFormatErrorMessage(paramName, value);
            LOG.error(message);
            return null;
        }
    }

    public int getIntValue(String paramName, int defaultValue) {
        Integer value = getIntValue(paramName);
        return value != null ? value : defaultValue;
    }

    public Long getRequiredLongValue(String paramName) throws ODCSFusionToolException {
        String value = getRequiredValue(paramName);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            String message = getNumberFormatErrorMessage(paramName, value);
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.MISSING_REQUIRED_PARAM, message);
        }
    }

    public Long getLongValue(String paramName) {
        String value = params.get(paramName);
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            String message = getNumberFormatErrorMessage(paramName, value);
            LOG.error(message);
            return null;
        }
    }

    public long getLongValue(String paramName, long defaultValue) {
        Long value = getLongValue(paramName);
        return value != null ? value : defaultValue;
    }

    private String getRequiredValue(String paramName) throws ODCSFusionToolException {
        String value = params.get(paramName);
        if (value == null) {
            throw new ODCSFusionToolException(ODCSFusionToolErrorCodes.MISSING_REQUIRED_PARAM,
                    "Missing required parameter " + paramName + " for input '" + sourceName + "'");
        }
        return value;
    }

    private String getNumberFormatErrorMessage(String paramName, String value) {
        return String.format(Locale.ROOT, "Value of parameter %s '%s' for input '%s' is not a valid number",
                paramName, value, sourceName);
    }
}
