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
@Root(name = "SourceGraphsRestriction")
public class SourceGraphsRestrictionXml {

    @Attribute(required = false)
    private String graphvar;

    @Text
    private String value;

    public String getGraphvar() {
        return graphvar;
    }

    public String getValue() {
        return value;
    }
}
