package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.LDFusionToolException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;

/**
 * Helper class for parsing string key-value parameters .
 */
public class ParamReader {
    private static final Logger LOG = LoggerFactory.getLogger(ParamReader.class);
    protected final Map<String, String> params;
    protected final String objectLabel;

    public ParamReader(Map<String, String> params, String parametrizedObjectLabel) {
        this.params = params;
        this.objectLabel = parametrizedObjectLabel;
    }

    public String getLabel() {
        return objectLabel;
    }

    public String getRequiredStringValue(String paramName) throws LDFusionToolException {
        return getRequiredValue(paramName);
    }

    public String getStringValue(String paramName) {
        return params.get(paramName);
    }

    public String getStringValue(String paramName, String defaultValue) {
        String value = params.get(paramName);
        return value != null ? value : defaultValue;
    }

    public Integer getRequiredIntValue(String paramName) throws LDFusionToolException {
        String value = getRequiredValue(paramName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new LDFusionToolException(getNumberFormatErrorMessage(paramName, value));
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

    public Long getRequiredLongValue(String paramName) throws LDFusionToolException {
        String value = getRequiredValue(paramName);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new LDFusionToolException(getNumberFormatErrorMessage(paramName, value));
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

    private String getRequiredValue(String paramName) throws LDFusionToolException {
        String value = params.get(paramName);
        if (value == null || value.length() == 0) {
            throw new LDFusionToolException(String.format(Locale.ROOT, "Missing required parameter %s for %s", paramName, objectLabel));
        }
        return value;
    }

    private String getNumberFormatErrorMessage(String paramName, String value) {
        return String.format(Locale.ROOT, "Value of parameter %s '%s' for %s is not a valid number",
                paramName, value, objectLabel);
    }
}
