/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import java.util.List;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "Config")
public class ConfigXml {

    @ElementList(name = "Prefixes", required = false)
    private List<PrefixXml> prefixes;

    @ElementList(name = "DataSources", empty = true, required = true)
    private List<DataSourceXml> dataSources;
    
    @Element(name="DataProcessing", required = false)
    private DataProcessingXml dataProcessing;

    @Element(name = "ConflictResolution", required = false)
    private ConflictResolutionXml conflictResolution;

    @ElementList(name = "Outputs")
    private List<OutputXml> outputs;

    public List<PrefixXml> getPrefixes() {
        return prefixes;
    }

    public List<DataSourceXml> getDataSources() {
        return dataSources;
    }
    
    public DataProcessingXml getDataProcessing() {
        return dataProcessing;
    }

    public ConflictResolutionXml getConflictResolution() {
        return conflictResolution;
    }

    public List<OutputXml> getOutputs() {
        return outputs;
    }
}
