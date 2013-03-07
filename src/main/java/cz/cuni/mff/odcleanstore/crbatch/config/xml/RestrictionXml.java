/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.Text;

/**
 * @author Jan Michelfeit
 */
@Root
public class RestrictionXml {

    @Attribute(required = false)
    private String var;

    @Text
    private String value;

    public String getVar() {
        return var;
    }

    public String getValue() {
        return value;
    }
}
