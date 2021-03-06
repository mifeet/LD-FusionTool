package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import com.google.common.collect.ImmutableList;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.LDFusionToolTestUtils;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.NTriplesParserSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
public class NTuplesParserTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @Test
    public void parsesCorrectlyAllValueTypes() throws Exception {
        // Arrange
        String inputString =
                "<http://uri1> _:bnode1 \"literal\" \"123\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                        "<http://uri2> .\n" +
                        "<http://uri3> <http://uri4> .";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(inputString.getBytes());
        ParserConfig parserConfig = new ParserConfig();
        parserConfig.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);

        // Act
        NTuplesParser parser = new NTuplesParser(new InputStreamReader(inputStream), parserConfig);
        List<List<Value>> result = new ArrayList<>();
        try {
            while (parser.hasNext()) {
                result.add(parser.next());
            }
        } finally {
            parser.close();
        }

        // Assert
        assertThat(result.get(0), is(Arrays.asList(
                VF.createURI("http://uri1"),
                VF.createBNode("bnode1"),
                VF.createLiteral("literal"),
                VF.createLiteral(123))));
        assertThat(result.get(1), is(Arrays.asList(
                (Value) VF.createURI("http://uri2"))));
        assertThat(result.get(2), is(Arrays.asList(
                (Value) VF.createURI("http://uri3"),
                VF.createURI("http://uri4"))));
    }

    @Test
    public void skipsCommentsAndNewlines() throws Exception {
        // Arrange
        String inputString = "<http://uri1> .\n"
                + "# comment\n"
                + "<http://uri2> .\n"
                + "\n\n\n"
                + "<http://uri3> .";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(inputString.getBytes());
        ParserConfig parserConfig = new ParserConfig();
        parserConfig.set(BasicParserSettings.PRESERVE_BNODE_IDS, true);

        // Act
        NTuplesParser parser = new NTuplesParser(new InputStreamReader(inputStream), parserConfig);
        List<List<Value>> result = new ArrayList<>();
        try {
            while (parser.hasNext()) {
                result.add(parser.next());
            }
        } finally {
            parser.close();
        }

        // Assert
        assertThat(result.get(0), is(Arrays.asList((Value) VF.createURI("http://uri1"))));
        assertThat(result.get(1), is(Arrays.asList((Value) VF.createURI("http://uri2"))));
        assertThat(result.get(2), is(Arrays.asList((Value) VF.createURI("http://uri3"))));
    }

    @Test(expected = IOException.class)
    public void throwsExceptionOnParseError() throws Exception {
        // Arrange
        String inputString = "<http://uri1> _bnode1 .\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(inputString.getBytes());

        // Act
        NTuplesParser parser = new NTuplesParser(new InputStreamReader(inputStream), new ParserConfig());
        try {
            while (parser.hasNext()) {
                parser.next();
            }
        } finally {
            parser.close();
        }
    }

    @Test
    public void skipsErrorWhenFailOnNTriplesInvalidLinesSettingIsNonFatal() throws Exception {
        // Arrange
        String inputString = "<http://uri1> _:bnode1 abc.\n<http://uri2> <http://uri3>.\n";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(inputString.getBytes());
        ParserConfig parserConfig = new ParserConfig();
        parserConfig.addNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);

        // Act
        NTuplesParser parser = new NTuplesParser(new InputStreamReader(inputStream), parserConfig);
        List<List<Value>> tuples = new ArrayList<>();
        try {
            while (parser.hasNext()) {
                tuples.add(parser.next());
            }
        } finally {
            parser.close();
        }

        // Assert
        assertThat(tuples, contains((List<Value>) ImmutableList.of((Value) LDFusionToolTestUtils.createHttpUri("uri2"), LDFusionToolTestUtils.createHttpUri("uri3"))));
    }

}