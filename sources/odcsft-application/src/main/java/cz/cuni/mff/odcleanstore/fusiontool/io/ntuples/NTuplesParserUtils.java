package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import org.openrdf.model.Value;

import java.io.IOException;
import java.util.Comparator;

/**
 * Helper methods for use with {@link NTuplesParser}.
 */
public final class NTuplesParserUtils {
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
}
