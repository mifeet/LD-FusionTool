/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationErrorStrategy;
import cz.cuni.mff.odcleanstore.conflictresolution.EnumCardinality;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root
public class ResolutionStrategyXml {
    @Attribute(name = "function", required = false)
    private String resolutionFunctionName;

    @Attribute(name = "cardinality", required = false)
    private EnumCardinality cardinality;
    
    @Attribute(name = "aggregationErrorStrategy", required = false)
    private EnumAggregationErrorStrategy aggregationErrorStrategy;

    @ElementList(required = false, inline = true)
    private List<ParamXml> params;

    public String getResolutionFunctionName() {
        return resolutionFunctionName;
    }

    public EnumCardinality getCardinality() {
        return cardinality;
    }
    
    public EnumAggregationErrorStrategy getAggregationErrorStrategy() {
        return aggregationErrorStrategy;
    }

    public List<ParamXml> getParams() {
        return params;
    }

}