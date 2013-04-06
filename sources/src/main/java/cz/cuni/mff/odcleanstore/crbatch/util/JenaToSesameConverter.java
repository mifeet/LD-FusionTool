/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.LiteralLabel;

import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * Utility class for (low-level) conversion from the Jena data model to Sesame (openrdf) data model.
 * @author Jan Michelfeit
 */
public final class JenaToSesameConverter {
    private static ValueFactory valueFactory = ValueFactoryImpl.getInstance();

    /**
     * Converts {@link Triple} to a {@link Statement}.
     * @param jenaTriple triple to convert
     * @return statement equivalent to the given jenaTriple
     * @throws ConversionException conversion error (e.g. trying to convert triple with a variable node)
     */
    public static Statement convertTriple(Triple jenaTriple) throws ConversionException {
        Resource sesameSubject = convertToResource(jenaTriple.getSubject());
        URI sesamePredicate = convertToURI(jenaTriple.getPredicate());
        Value sesameObject = convertToValue(jenaTriple.getObject());

        return valueFactory.createStatement(sesameSubject,  sesamePredicate, sesameObject);
    }
    
    /**
     * Converts {@link Quad} to a {@link Statement}.
     * @param quad quad to convert
     * @return statement equivalent to the given quad
     * @throws ConversionException conversion error (e.g. trying to convert triple with a variable node)
     */
    public static Statement convertQuad(Quad quad) throws ConversionException {
        Resource sesameSubject = convertToResource(quad.getSubject());
        URI sesamePredicate = convertToURI(quad.getPredicate());
        Value sesameObject = convertToValue(quad.getObject());
        Resource context = convertToResource(quad.getGraphName());

        return valueFactory.createStatement(sesameSubject,  sesamePredicate, sesameObject, context);
    }

    /**
     * Converts {@link Node} to an equivalent {@link Value}.
     * @param jenaNode node to convert
     * @return value equivalent to the given node
     * @throws ConversionException conversion error (e.g. trying to convert a variable node)
     */
    public static Value convertToValue(Node jenaNode) throws ConversionException {
        if (jenaNode.isURI()) {
            return valueFactory.createURI(jenaNode.getURI());
        } else if (jenaNode.isBlank()) {
            return valueFactory.createBNode(jenaNode.getBlankNodeLabel());
        } else if (jenaNode.isLiteral()) {
            return convertToLiteral(jenaNode);
        } else {
            throw new ConversionException("Cannot convert Node of type " + jenaNode.getClass().getSimpleName() + " to Value");
        }
    }
    
    /**
     * Converts {@link Node} to an equivalent {@link URI}. 
     * @param jenaNode node to convert
     * @return URI equivalent to the given node
     * @throws ConversionException given node is not an URI resource
     */
    public static URI convertToURI(Node jenaNode) throws ConversionException {
        if (!jenaNode.isURI()) {
            throw new ConversionException("Cannot convert Node of type " + jenaNode.getClass().getSimpleName() + " to URI");
        }
        return valueFactory.createURI(jenaNode.getURI());
    }
    
    /**
     * Converts {@link Node} to an equivalent {@link Resource}. 
     * @param jenaNode node to convert
     * @return URI equivalent to the given node
     * @throws ConversionException given node is not an URI resource or blank node
     */
    public static Resource convertToResource(Node jenaNode) throws ConversionException {
        if (!jenaNode.isURI() && !jenaNode.isBlank()) {
            throw new ConversionException("Cannot convert Node of type " + jenaNode.getClass().getSimpleName() + " to Resource");
        }
        return (Resource) convertToValue(jenaNode);
    }
    
    /**
     * Converts {@link Node} to an equivalent {@link Literal}. 
     * @param jenaNode node to convert
     * @return Literal equivalent to the given node
     * @throws ConversionException given node is not a literal
     */
    public static Literal convertToLiteral(Node jenaNode) throws ConversionException {
        if (!jenaNode.isLiteral()) {
            throw new ConversionException("Cannot convert Node of type " + jenaNode.getClass().getSimpleName() + " to Literal");
        }
        LiteralLabel jenaLiteral = jenaNode.getLiteral();
        if (jenaLiteral.getDatatype() != null) {
            URI sesameDatatype = valueFactory.createURI(jenaLiteral.getDatatypeURI());
            return valueFactory.createLiteral(jenaLiteral.getLexicalForm(), sesameDatatype);
        } else if (jenaLiteral.language() != null && !"".equals(jenaLiteral.language())) {
            return valueFactory.createLiteral(jenaLiteral.getLexicalForm(), jenaLiteral.language());
        } else {
            return valueFactory.createLiteral(jenaLiteral.getLexicalForm());
        }
    }
    
    /**
     * Hidden constructor of a utility class.
     */
    private JenaToSesameConverter() {
    }
}
