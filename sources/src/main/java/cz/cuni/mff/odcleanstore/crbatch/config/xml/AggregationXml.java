/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

import cz.cuni.mff.odcleanstore.conflictresolution.EnumAggregationType;

/**
 * @author Jan Michelfeit
 */
@Root(name = "Aggregation")
public class AggregationXml {

    @Attribute(required = false)
    private EnumAggregationType type;

    @Attribute(required = false)
    private Boolean multivalue;

    @ElementList(required = false, empty = false, inline = true)
    private List<PropertyXml> properties;

    public EnumAggregationType getType() {
        return type;
    }

    public Boolean getMultivalue() {
        return multivalue;
    }

    public List<PropertyXml> getProperties() {
        return properties;
    }

}
