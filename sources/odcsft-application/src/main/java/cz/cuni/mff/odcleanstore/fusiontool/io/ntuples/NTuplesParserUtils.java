package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator;
import org.openrdf.model.Resource;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFParseException;

import java.io.IOException;
import java.util.Comparator;

/**
 * Helper methods for use with {@link NTuplesParser}.
 */
public final class NTuplesParserUtils {
    public static final ValueComparator VALUE_COMPARATOR = new ValueComparator();
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    /**
     * Returns true if calling {@link NTuplesParser#next()} on the given
     * parser would return a tuple which has its first item equal to {@code comparedFirstValue}.
     * @param parser parser to examine
     * @param comparedFirstValue value to be matched against
     * @return true if calling {@link NTuplesParser#next()} on the given
     * parser would return a tuple which has its first item equal to {@code comparedFirstValue}
     * @throws java.io.IOException parser error
     */
    public static boolean hasMatchingRecord(NTuplesParser parser, Value comparedFirstValue) throws IOException {
        return parser.hasNext() && !parser.peek().isEmpty() && parser.peek().get(0).equals(comparedFirstValue);
    }

    /**
     * Skips all records in the parser that have the first component of the tuple less than compared value.
     * After execution of this method, {@code parser} will point to the first record with first component greater
     * or equal to {@code comparedFirstValue} or beyond the end of file if there are no more records
     * @param parser parser to move forward
     * @param comparedFirstValue compared value
     * @param valueComparator comparator to be used for value comparison
     * @return true if the value returned by {@code parser.next()} will be equal to {@code comparedFirstValue}, false otherwise
     * @throws java.io.IOException parser error
     */
    public static boolean skipLessThan(NTuplesParser parser, Value comparedFirstValue, Comparator<Value> valueComparator)
            throws IOException {
        int cmp = -1;
        while (parser.hasNext() && !parser.peek().isEmpty()
                && (cmp = valueComparator.compare(parser.peek().get(0), comparedFirstValue)) < 0) {
            parser.next();
        }
        return cmp == 0;
    }

    /**
     * Parses an URI or blank node in NTriples format.
     * Any characters after the first URI or blank node are ignored.
     * Assumes the input is valid, doesn't do much validation.
     * @param str string to parse
     * @return parsed URI or blank node
     * @throws org.openrdf.rio.RDFParseException parse error
     */
    public static Resource parseValidResource(String str) throws RDFParseException {
        int length = str.length();
        if (length < 3) {
            throw new RDFParseException("String '" + str + "' is not a valid URI nor blank node");
        }
        char firstChar = str.charAt(0);
        if (firstChar == '<') {
            int endIndex = str.indexOf('>', 1);
            if (endIndex < 0) {
                throw new RDFParseException("Expected '>' but none found in '" + str + "'");
            }
            String uri = str.substring(1, endIndex);
            return VF.createURI(uri);
        } else if (firstChar == '_' && str.charAt(1) == ':') {
            int endIndex = 2;
            while (endIndex < length && str.charAt(endIndex) != ' ' && str.charAt(endIndex) != '\t') {
                endIndex++;
            }
            while (str.charAt(endIndex - 1) == '.') {
                endIndex--; // dot mustn't be the last character
            }
            String nodeId = str.substring(2, endIndex);
            return VF.createBNode(nodeId);
        } else {
            throw new RDFParseException(String.format("Expected '>' or '_:' but found '%s' in '%s'", str.charAt(0), str));
        }
    }
}
