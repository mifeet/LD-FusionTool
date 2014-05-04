package cz.cuni.mff.odcleanstore.fusiontool.util;

import com.google.code.externalsorting.StringSizeEstimator;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

/**
 * Simple class used to estimate memory usage of {@link org.openrdf.model.Statement} objects.
 */
public final class StatementSizeEstimator {
    private static int INT_SIZE = 8;
    private static int STATEMENT_OVERHEAD;
    private static int OBJ_HEADER;
    private static int OBJ_REF;
    private static int URI_BNODE_OVERHEAD;
    private static int LITERAL_OVERHEAD;

    private static boolean IS_64_BIT_JVM;

    /**
     * Class initializations.
     */
    static {
        // By default we assume 64 bit JVM (defensive approach since we will get larger estimations in case we are not sure)
        IS_64_BIT_JVM = true;
        // check the system property "sun.arch.data.model", not very safe, as it might not work for all JVM implementations
        // nevertheless the worst thing that might happen is that the JVM is 32bit but we assume its 64bit,
        // so we will be counting a few extra bytes per string object no harm done here since this is just an approximation.
        String arch = System.getProperty("sun.arch.data.model");
        if (arch != null) {
            if (arch.indexOf("32") != -1) {
                // If exists and is 32 bit then we assume a 32bit JVM
                IS_64_BIT_JVM = false;
            }
        }
        // The sizes below are a bit rough as we don't take into account advanced JVM options such as compressed oops
        // however if our calculation is not accurate it'll be a bit over so there is no danger of an out of memory error because of this.
        OBJ_HEADER = IS_64_BIT_JVM ? 16 : 8;
        OBJ_REF = IS_64_BIT_JVM ? 8 : 4;
        URI_BNODE_OVERHEAD = (OBJ_HEADER + OBJ_REF + INT_SIZE);
        LITERAL_OVERHEAD = OBJ_HEADER + 3 * OBJ_REF;
        STATEMENT_OVERHEAD =  OBJ_HEADER + 4 * OBJ_REF;
    }

    /**
     * Estimates the size of a {@link org.openrdf.model.Statement} object + a reference to it in bytes.
     * We assume default Sesame implementation, and literals only in the place of object.
     * Try to overestimate where possible.
     *
     * @param statement The Statement to estimate memory footprint.
     * @return The <strong>estimated</strong> size in bytes.
     */
    public static long estimatedSizeOf(Statement statement) {
        Value object = statement.getObject();
        Resource context = statement.getContext();
        long result = STATEMENT_OVERHEAD
                + 2 * URI_BNODE_OVERHEAD // subject, predicate
                + OBJ_REF // reference to the statement
                + StringSizeEstimator.estimatedSizeOf(statement.getSubject().stringValue())
                + StringSizeEstimator.estimatedSizeOf(statement.getPredicate().stringValue());

        // object
        if (object instanceof Literal) {
            Literal literal = (Literal) object;
            result += LITERAL_OVERHEAD + StringSizeEstimator.estimatedSizeOf(literal.stringValue());
            if (literal.getLanguage() != null) {
                result += StringSizeEstimator.estimatedSizeOf(literal.getLanguage());
            }
            if (literal.getDatatype() != null) {
                result += URI_BNODE_OVERHEAD + StringSizeEstimator.estimatedSizeOf(literal.getDatatype().stringValue());
            }
        } else {
            result += URI_BNODE_OVERHEAD + StringSizeEstimator.estimatedSizeOf(object.stringValue());
        }

        // context
        if (context != null) {
            result += URI_BNODE_OVERHEAD + StringSizeEstimator.estimatedSizeOf(context.stringValue());
        }

        return result;
    }
}

