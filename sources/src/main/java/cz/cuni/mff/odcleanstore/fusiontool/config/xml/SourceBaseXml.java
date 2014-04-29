/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root
public abstract class SourceBaseXml {
    @Attribute
    protected String type;
    
    @Attribute(required = false)
    protected String name;

    @ElementList(empty = false, inline = true, required = false)
    protected List<ParamXml> params;

    public String getType() {
        return type;
    }
    
    public String getName() {
        return name;
    }

    public List<ParamXml> getParams() {
        return params;
    }
}
