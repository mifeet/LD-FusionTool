/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

/**
 * @author Jan Michelfeit
 */
@Root(name = "ConflictResolution")
public class ConflictResolutionXml {
    @ElementList(name = "DefaultSettings", required = false)
    private List<ParamXml> defaultSettings;

    @ElementList(name = "PropertySettings", required = false)
    private List<AggregationXml> propertySettings;

    public List<ParamXml> getDefaultSettings() {
        return defaultSettings;
    }

    public List<AggregationXml> getPropertySettings() {
        return propertySettings;
    }

}
