/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "DataSource")
public class DataSourceXml {
    @Attribute
    private String type;
    
    @Attribute(required = false)
    private String name;

    @ElementList(empty = false, inline = true, required = false)
    private List<ParamXml> params;

    @Element(name = "GraphRestriction", required = false)
    private RestrictionXml graphRestriction;

    @Element(name = "MetadataGraphRestriction", required = false)
    private RestrictionXml metadataGraphRestriction;

    public String getType() {
        return type;
    }
    
    public String getName() {
        return name;
    }

    public List<ParamXml> getParams() {
        return params;
    }
    
    public RestrictionXml getGraphRestriction() {
        return graphRestriction;
    }

    public RestrictionXml getMetadataGraphRestriction() {
        return metadataGraphRestriction;
    }
}
