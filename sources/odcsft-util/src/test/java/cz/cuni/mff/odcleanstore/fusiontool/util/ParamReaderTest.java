package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import org.junit.Test;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ParamReaderTest {
    @Test
    public void returnsStringValueWhenStringParamPresent() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "value1");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        String result = paramReader.getStringValue("key1");

        // Assert
        assertThat(result, equalTo("value1"));
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredStringParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredStringValue("key1");
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredStringParamEmpty() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredStringValue("key1");
    }

    @Test()
    public void returnsNullWhenNonRequiredStringParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        String result = paramReader.getStringValue("key1");

        // Assert
        assertThat(result, nullValue());
    }

    @Test
    public void returnsStringValueWhenStringParamPresentAndDefaultGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "value1");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        String result = paramReader.getStringValue("key1", "default");

        // Assert
        assertThat(result, equalTo("value1"));
    }

    @Test
    public void returnsDefaultValueWhenStringParamNotPresentAndDefaultGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        String result = paramReader.getStringValue("key1", "default");

        // Assert
        assertThat(result, equalTo("default"));
    }

    @Test
    public void returnsIntValueWhenValidIntParamPresent() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Integer result = paramReader.getIntValue("key1");

        // Assert
        assertThat(result, equalTo(123));
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredIntParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredIntValue("key1");
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredIntParamMalformed() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123s4");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredIntValue("key1");
    }

    @Test()
    public void returnsNullWhenNonRequiredIntParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Integer result = paramReader.getIntValue("key1");

        // Assert
        assertThat(result, nullValue());
    }

    @Test
    public void returnsNullWhenNonRequiredIntParamMalformed() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123s4");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Integer result = paramReader.getIntValue("key1");

        // Assert
        assertThat(result, nullValue());
    }

    @Test
    public void returnsIntValueWhenValidIntParamPresentAndDefaultValueGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Integer result = paramReader.getIntValue("key1", 1000);

        // Assert
        assertThat(result, equalTo(123));
    }

    @Test
    public void returnsDefaultValueWhenMalformedIntParamAndDefaultValueGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "12s3");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Integer result = paramReader.getIntValue("key1", 1000);

        // Assert
        assertThat(result, equalTo(1000));
    }

    @Test
    public void returnsLongValueWhenValidLongParamPresent() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Long result = paramReader.getLongValue("key1");

        // Assert
        assertThat(result, equalTo(123l));
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredLongParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredLongValue("key1");
    }

    @Test(expected = ODCSFusionToolException.class)
    public void throwsWhenRequiredLongParamMalformed() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123s4");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        paramReader.getRequiredLongValue("key1");
    }

    @Test()
    public void returnsNullWhenNonRequiredLongParamMissing() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Long result = paramReader.getLongValue("key1");

        // Assert
        assertThat(result, nullValue());
    }

    @Test
    public void returnsNullWhenNonRequiredLongParamMalformed() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123s4");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Long result = paramReader.getLongValue("key1");

        // Assert
        assertThat(result, nullValue());
    }

    @Test
    public void returnsLongValueWhenValidLongParamPresentAndDefaultValueGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "123");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Long result = paramReader.getLongValue("key1", 1000l);

        // Assert
        assertThat(result, equalTo(123l));
    }

    @Test
    public void returnsDefaultValueWhenMalformedLongParamAndDefaultValueGiven() throws Exception {
        // Arrange
        Map<String, String> params = newHashMap();
        params.put("key1", "12s3");
        params.put("key2", "value2");

        // Act
        ParamReader paramReader = new ParamReader(params, "object");
        Long result = paramReader.getLongValue("key1", 1000l);

        // Assert
        assertThat(result, equalTo(1000l));
    }
}