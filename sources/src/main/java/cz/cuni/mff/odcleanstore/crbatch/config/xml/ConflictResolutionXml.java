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
    @ElementList(name = "Params", required = false)
    private List<ParamXml> params;
    
    @ElementList(name = "DefaultAggregation", required = false)
    private List<ParamXml> defaultAggregation;

    @ElementList(name = "PropertyAggregations", required = false)
    private List<AggregationXml> propertyAggregations;

    public List<ParamXml> getParams() {
        return params;
    }
    
    public List<ParamXml> getDefaultAggregation() {
        return defaultAggregation;
    }

    public List<AggregationXml> getPropertyAggregations() {
        return propertyAggregations;
    }

}
