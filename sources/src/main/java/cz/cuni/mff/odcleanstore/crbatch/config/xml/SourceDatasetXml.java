/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config.xml;

import org.simpleframework.xml.Element;
import org.simpleframework.xml.Root;

// CHECKSTYLE:OFF

/**
 * @author Jan Michelfeit
 */
@Root(name = "SourceDataset")
public class SourceDatasetXml {
    @Element(name = "GraphsRestriction", required = false)
    private RestrictionXml graphsRestriction;

    @Element(name = "OntologyRestriction", required = false)
    private RestrictionXml ontologyRestriction;

    @Element(name = "SeedResourceRestriction", required = false)
    private RestrictionXml seedResourceRestriction;

    public RestrictionXml getGraphsRestriction() {
        return graphsRestriction;
    }

    public RestrictionXml getOntologyRestriction() {
        return ontologyRestriction;
    }

    public RestrictionXml getSeedResourceRestriction() {
        return seedResourceRestriction;
    }
}
