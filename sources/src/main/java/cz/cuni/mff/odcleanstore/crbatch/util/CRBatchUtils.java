package cz.cuni.mff.odcleanstore.crbatch.util;

import com.hp.hpl.jena.graph.Node;

import cz.cuni.mff.odcleanstore.shared.ODCSUtils;

/**
 * Various utility methods.
 * 
 * @author Jan Michelfeit
 */
public final class CRBatchUtils {
    /** Time unit 60. */
    public static final long TIME_UNIT_60 = 60;

    /**
     * Returns an URI representing the given node or null if it is not a resource.
     * For blank nodes returns the Virtuoso blank node identifier.
     * @param node RDF node
     * @return URI representing
     */
    public static String getNodeURI(Node node) {
        if (node.isURI()) {
            return node.getURI();
        } else if (node.isBlank()) {
            return ODCSUtils.getVirtuosoURIForBlankNode(node);
        } else {
            return null;
        }
    }

    /** Disable constructor for a utility class. */
    private CRBatchUtils() {
    }
}
