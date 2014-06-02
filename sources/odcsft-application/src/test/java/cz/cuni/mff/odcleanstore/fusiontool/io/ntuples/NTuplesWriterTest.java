package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class NTuplesWriterTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @Test
    public void writesCorrectlyAllValueTypes() throws Exception {
        // Arrange
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        // Act
        NTuplesWriter nTuplesWriter = new NTuplesWriter(new OutputStreamWriter(outputStream));
        try {
            nTuplesWriter.writeTuple(VF.createURI("http://uri1"), VF.createBNode("bnode1"), VF.createLiteral("literal"), VF.createLiteral(123));
            nTuplesWriter.writeTuple(VF.createURI("http://uri2"));
            nTuplesWriter.writeTuple(VF.createURI("http://uri3"), VF.createURI("http://uri4"));
        } finally {
            nTuplesWriter.close();
        }

        // Assert
        String expectedResult =
                "<http://uri1> _:bnode1 \"literal\" \"123\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://uri2> .\n" +
                "<http://uri3> <http://uri4> .\n";
        String result = outputStream.toString();
        assertThat(result, is(expectedResult));
    }
}