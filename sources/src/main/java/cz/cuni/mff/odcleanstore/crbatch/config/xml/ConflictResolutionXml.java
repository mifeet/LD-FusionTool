/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "ConflictResolution")
public class ConflictResolutionXml {
    @ElementList(name = "DefaultAggregation", required = false)
    private List<ParamXml> defaultAggregation;

    @ElementList(name = "PropertyAggregations", required = false)
    private List<AggregationXml> propertyAggregations;

    public List<ParamXml> getDefaultAggregation() {
        return defaultAggregation;
    }

    public List<AggregationXml> getPropertyAggregations() {
        return propertyAggregations;
    }

}
