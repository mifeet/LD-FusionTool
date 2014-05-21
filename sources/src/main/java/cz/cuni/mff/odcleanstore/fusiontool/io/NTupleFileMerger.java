package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator;
import org.openrdf.model.Value;
import org.openrdf.rio.ParserConfig;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 */
public class NTupleFileMerger {
    private final NTupleMergeTransform transform;
    private final ParserConfig parserConfig;
    private static final ValueComparator comparator = new ValueComparator();

    public NTupleFileMerger(NTupleMergeTransform transform, ParserConfig parserConfig) {
        this.transform = transform;
        this.parserConfig = parserConfig;
    }

    /**
     * Merges two files serialized with format used by {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter}
     * using full inner join by the first item in each tuple.
     * Necessary buffering is done on the right side.
     * After join of corresponding lines, the matching records are transformed with transformed and written to {@code outputWriter}.
     * The method expects that both input files are sorted by first item on each line.
     * @param leftReader reader of file serialized with {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter}
     * and sorted by first {@code Value} on each  line using {@link cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator}
     * @param rightReader reader of file serialized with {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesWriter}
     * and sorted by first {@code Value} on each  line using {@link cz.cuni.mff.odcleanstore.conflictresolution.impl.util.ValueComparator}
     * @param outputWriter writer for merged result
     */
    public void merge(Reader leftReader, Reader rightReader, Writer outputWriter) throws IOException {
        NTuplesParser leftParser = new NTuplesParser(leftReader, parserConfig);
        NTuplesParser rightParser = new NTuplesParser(rightReader, parserConfig);
        NTuplesWriter output = new NTuplesWriter(outputWriter);
        List<List<Value>> rightBuffer = new ArrayList<List<Value>>();
        try {
            while (leftParser.hasNext() && rightParser.hasNext()) {
                boolean wasEqual = skipLessThan(leftParser, rightParser.peek());
                if (!wasEqual && leftParser.hasNext()) {
                    wasEqual = skipLessThan(rightParser, leftParser.peek());
                }

                if (wasEqual) {
                    rightBuffer.clear();
                    readToBuffer(rightParser, rightBuffer);
                    mergeWithBuffer(leftParser.next(), rightBuffer, output);
                    while (leftParser.hasNext() && !rightBuffer.isEmpty() && compare(leftParser.peek(), rightBuffer.get(0)) == 0) {
                        mergeWithBuffer(leftParser.next(), rightBuffer, output);
                    }
                }
            }
        } finally {
            output.close();
            leftParser.close();
            rightParser.close();
        }
    }

    private boolean skipLessThan(NTuplesParser parser, List<Value> compared) throws IOException {
        int cmp = -1;
        while (parser.hasNext()
                && (cmp = compare(parser.peek(), compared)) < 0) {
            parser.next();
        }
        return cmp == 0;
    }

    private void mergeWithBuffer(List<Value> left, List<List<Value>> rightBuffer, NTuplesWriter output) throws IOException {
        for (List<Value> right : rightBuffer) {
            output.writeTuple(transform.transform(left, right));
        }
    }

    private void readToBuffer(NTuplesParser rightParser, List<List<Value>> rightBuffer) throws IOException {
        if (!rightParser.hasNext()) {
            return;
        }
        List<Value> first = rightParser.next();
        rightBuffer.add(first);
        while (rightParser.hasNext() && compare(first, rightParser.peek()) == 0) {
            rightBuffer.add(rightParser.next());
        }
    }

    private int compare(List<Value> left, List<Value> right) {
        return comparator.compare(left.get(0), right.get(0));
    }

    public static interface NTupleMergeTransform {
        Value[] transform(List<Value> leftValues, List<Value> rightValues);
    }
}
