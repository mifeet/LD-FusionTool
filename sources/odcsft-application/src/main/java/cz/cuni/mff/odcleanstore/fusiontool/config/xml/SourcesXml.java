/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config.xml;

import java.util.List;

import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Sources")
public class SourcesXml {
    @ElementList(name = "DataSource", required = true, inline = true)
    private List<DataSourceXml> dataSources;
    
    @ElementList(name = "SameAsSource", required = false, inline = true, empty = false)
    private List<SameAsSourceXml> sameAsSources;
    
    @ElementList(name = "MetadataSource", required = false, inline = true, empty = false)
    private List<MetadataSourceXml> metadataSources;

    public List<DataSourceXml> getDataSources() {
        return dataSources;
    }

    public List<SameAsSourceXml> getSameAsSources() {
        return sameAsSources;
    }

    public List<MetadataSourceXml> getMetadataSources() {
        return metadataSources;
    }
}
