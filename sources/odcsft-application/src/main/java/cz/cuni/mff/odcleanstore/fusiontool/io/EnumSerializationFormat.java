/**
 *
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import org.openrdf.rio.RDFFormat;

/**
 * Type of RDF serialization.
 * @author Jan Michelfeit
 */
public enum EnumSerializationFormat {
    /**
     * File in RDF/XML.
     */
    RDF_XML(RDFFormat.RDFXML),

    /**
     * File in N3.
     */
    N3(RDFFormat.N3),

    /**
     * File in TriG.
     */
    TRIG(RDFFormat.TRIG),

    /**
     * File in N-Quads
     */
    NQUADS(RDFFormat.NQUADS),

    /**
     * File in HTML.
     */
    HTML(null);

    private final RDFFormat sesameFormat;

    private EnumSerializationFormat(RDFFormat sesameFormat) {
        this.sesameFormat = sesameFormat;
    }

    /**
     * Returns the corresponding RDF format in Sesame.
     * @return sesame {@link RDFFormat}
     */
    public RDFFormat toSesameFormat() {
        return sesameFormat;
    }

    /**
     * Converts string to an enum value.
     * This method is more liberal than valueOf().
     * @param str string to convert
     * @return converted value or null
     */
    public static EnumSerializationFormat parseFormat(String str) {
        if ("nquads".equalsIgnoreCase(str)) {
            return EnumSerializationFormat.NQUADS;
        } else if ("n-quads".equalsIgnoreCase(str)) {
            return EnumSerializationFormat.NQUADS;
        } else if ("ntriples".equalsIgnoreCase(str)) {
            return EnumSerializationFormat.N3;
        } else if ("rdf/xml".equalsIgnoreCase(str) || "rdfxml".equalsIgnoreCase(str)) {
            return EnumSerializationFormat.RDF_XML;
        } else {
            try {
                return valueOf(str.toUpperCase());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }
}
