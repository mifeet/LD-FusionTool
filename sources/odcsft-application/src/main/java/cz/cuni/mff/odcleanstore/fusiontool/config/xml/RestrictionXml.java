/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.Text;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root
public class RestrictionXml {
    // @Attribute(required = false)
    @Attribute
    protected String var;

    @Text
    protected String value;

    public String getVar() {
        return var;
    }

    public String getValue() {
        return value;
    }
}
