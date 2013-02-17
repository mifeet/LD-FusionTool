/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

/**
 * @author Jan Michelfeit
 */
public enum EnumOutputFormat {
    RDF_XML("rdf"),
    N3("n3");
    
    private final String fileExtension;
    
    private EnumOutputFormat(String fileExtension) {
        this.fileExtension = fileExtension;
    }
    
    public String getFileExtension() {
        return fileExtension;
    }
}
