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
@Root(name = "Output")
public class OutputXml {
    @Attribute
    private String type;

    @ElementList(empty = false, inline = true, required = false)
    private List<ParamXml> params;

    public String getType() {
        return type;
    }

    public List<ParamXml> getParams() {
        return params;
    }

}
