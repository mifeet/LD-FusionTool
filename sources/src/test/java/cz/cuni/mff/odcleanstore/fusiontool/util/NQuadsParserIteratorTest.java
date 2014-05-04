package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.ODCSFTTestUtils;
import org.junit.Test;
import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;

import static cz.cuni.mff.odcleanstore.fusiontool.ContextAwareStatementIsEqual.contextAwareStatementIsEqual;
import static cz.cuni.mff.odcleanstore.fusiontool.ODCSFTTestUtils.createHttpStatement;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class NQuadsParserIteratorTest {
    @Test
    public void iteratesOverStatementsWhenParsingValidFile() throws Exception {
        // Arrange
        ArrayList<Statement> statements = new ArrayList<Statement>();
        statements.add(createHttpStatement("s1", "p1", "o1", "g1"));
        statements.add(createHttpStatement("s2", "p1", "o1", "g2"));
        statements.add(createHttpStatement("s3", "p1", "o3", "g1"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Rio.write(statements, outputStream, RDFFormat.NQUADS);
        outputStream.close();

        // Act
        Reader inputReader = new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()));
        NQuadsParserIterator parserIterator = new NQuadsParserIterator(inputReader);
        ArrayList<Statement> result = new ArrayList<Statement>();
        while (parserIterator.hasNext()) {
            result.add(parserIterator.next());
        }

        // Assert
        assertThat(result.size(), equalTo(statements.size()));
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.get(i), contextAwareStatementIsEqual(statements.get(i)));
        }
    }

    @Test
    public void iteratesOverStatementsWhenCommentIsInTheMiddle() throws Exception {
        // Arrange
        Statement statement1 = createHttpStatement("s1", "p1", "o1", "g1");
        Statement statement2 = createHttpStatement("s2", "p1", "o1", "g2");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RDFWriter writer = Rio.createWriter(RDFFormat.NQUADS, outputStream);
        writer.startRDF();
        writer.handleStatement(statement1);
        writer.handleComment("comment");
        writer.handleStatement(statement2);
        writer.endRDF();
        outputStream.close();

        // Act
        Reader inputReader = new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()));
        NQuadsParserIterator parserIterator = new NQuadsParserIterator(inputReader);
        ArrayList<Statement> result = new ArrayList<Statement>();
        while (parserIterator.hasNext()) {
            result.add(parserIterator.next());
        }

        // Assert
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), contextAwareStatementIsEqual(statement1));
        assertThat(result.get(1), contextAwareStatementIsEqual(statement2));
    }

    @Test(expected = Exception.class)
    public void throwsExceptionOnSyntacticError() throws Exception {
        // Arrange
        ArrayList<Statement> statements = new ArrayList<Statement>();
        statements.add(createHttpStatement("s1", "p1", "o1", "g1"));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Rio.write(statements, outputStream, RDFFormat.NQUADS);
        outputStream.write(";".getBytes());
        outputStream.close();

        // Act
        Reader inputReader = new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()));
        NQuadsParserIterator parserIterator = new NQuadsParserIterator(inputReader);
        while (parserIterator.hasNext()) {
            parserIterator.next();
        }
    }

    @Test
    public void closesInputStreamWhenClosed() throws Exception {
        // Arrange
        Reader inputReader = mock(Reader.class);

        // Act
        NQuadsParserIterator parserIterator = new NQuadsParserIterator(inputReader);
        parserIterator.close();

        // Assert
        verify(inputReader).close();
    }
}