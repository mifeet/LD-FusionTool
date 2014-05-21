package cz.cuni.mff.odcleanstore.fusiontool.io;

import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class NTuplesFileMergerTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    private NTuplesFileMerger.NTupleMergeTransform transform = new NTuplesFileMerger.NTupleMergeTransform() {
        @Override
        public Value[] transform(List<Value> leftValues, List<Value> rightValues) {
            ArrayList<Value> result = new ArrayList<Value>(leftValues);
            result.addAll(rightValues);
            return result.toArray(new Value[result.size()]);
        }
    };

    @Test
    public void mergesMatchingRecords() throws Exception {
        String leftInputStr = "<http://a> <http://a1> .\n" +
                "<http://b> <http://b1> .\n" +
                "<http://d> <http://d1> .\n" +
                "<http://e> <http://e1> .\n" +
                "<http://e> <http://e2> .\n" +
                "<http://f> <http://f1> .\n" +
                "<http://f> <http://f2> .\n";
        String rightInputStr = "<http://b> <http://b1> .\n" +
                "<http://c> <http://c1> .\n" +
                "<http://d> <http://d1> .\n" +
                "<http://d> <http://d2> .\n" +
                "<http://e> <http://e1> .\n" +
                "<http://f> <http://f1> .\n" +
                "<http://f> <http://f2> .\n";
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        InputStream leftInput = new ByteArrayInputStream(leftInputStr.getBytes());
        InputStream rightInput = new ByteArrayInputStream(rightInputStr.getBytes());

        // Act
        NTuplesFileMerger merger = new NTuplesFileMerger(transform, new ParserConfig());
        merger.merge(new InputStreamReader(leftInput), new InputStreamReader(rightInput), new OutputStreamWriter(output));

        // Assert
        String expectedOutput = "<http://b> <http://b1> <http://b> <http://b1> .\n" +
                "<http://d> <http://d1> <http://d> <http://d1> .\n" +
                "<http://d> <http://d1> <http://d> <http://d2> .\n" +
                "<http://e> <http://e1> <http://e> <http://e1> .\n" +
                "<http://e> <http://e2> <http://e> <http://e1> .\n" +
                "<http://f> <http://f1> <http://f> <http://f1> .\n" +
                "<http://f> <http://f1> <http://f> <http://f2> .\n" +
                "<http://f> <http://f2> <http://f> <http://f1> .\n" +
                "<http://f> <http://f2> <http://f> <http://f2> .\n";
        NTuplesParser outputParser = new NTuplesParser(new InputStreamReader(new ByteArrayInputStream(output.toByteArray())), new ParserConfig());
        NTuplesParser expectedOutputParser = new NTuplesParser(new InputStreamReader(new ByteArrayInputStream(expectedOutput.getBytes())), new ParserConfig());
        while (outputParser.hasNext() || expectedOutputParser.hasNext()) {
            assertThat(outputParser.hasNext(), is(expectedOutputParser.hasNext()));
            assertThat(outputParser.next(), is(expectedOutputParser.next()));
        }
    }
}