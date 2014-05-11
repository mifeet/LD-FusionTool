package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import cz.cuni.mff.odcleanstore.fusiontool.util.ThrowingAbstractIterator;
import org.openrdf.model.Statement;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.nquads.NQuadsParser;

import java.io.IOException;
import java.io.Reader;

/**
 * Iterator over quads that are dynamically parsed from an underlying
 * N-Quads serialization.
 */
public class NQuadsParserIterator
        extends ThrowingAbstractIterator<Statement, Exception>
        implements Closeable<IOException> {

    private Parser internalParser;

    public NQuadsParserIterator(Reader inputReader, ParserConfig parserConfig) throws IOException {
        this.internalParser = new Parser(inputReader, parserConfig);
    }

    @Override
    protected Statement computeNext() throws Exception {
        Statement next = internalParser.parseNext();
        if (next == null) {
            return endOfData();
        } else {
            return next;
        }
    }

    @Override
    public void close() throws IOException {
        if (internalParser != null) {
            internalParser.close();
        }
    }

    private static class Parser extends NQuadsParser implements Closeable<IOException> {
        private int c;
        private Statement currentStatement = null;

        public Parser(Reader inputReader, ParserConfig parserConfig) throws IOException {
            this.reader = inputReader;
            initialize();
            this.setParserConfig(parserConfig);
        }

        public void initialize() throws IOException {
            lineNo = 1;
            reportLocation(lineNo, 1);
            c = reader.read();
            c = skipWhitespace(c);
        }

        public Statement parseNext() throws IOException, RDFParseException {
            currentStatement = null;
            while (c != -1 && currentStatement == null) {
                if (c == '#') {
                    c = skipLine(c); // Comment, ignore
                } else if (c == '\r' || c == '\n') {
                    c = skipLine(c); // Empty line, ignore
                } else {
                    c = parseQuad(c);
                }

                c = skipWhitespace(c);
            }
            return currentStatement;
        }

        @Override
        public void close() throws IOException {
            clear();
            reader.close();
        }

        private int parseQuad(int c) throws IOException, RDFParseException {
            // TODO: fix error handling && ParserConfig
            c = parseSubject(c);
            c = skipWhitespace(c);
            c = parsePredicate(c);
            c = skipWhitespace(c);
            c = parseObject(c);
            c = skipWhitespace(c);
            if (c != '.') {
                c = this.parseContext(c);
                c = skipWhitespace(c);
            }
            if (c == -1) {
                throwEOFException();
            } else if (c != '.') {
                reportFatalError("Expected '.', found: " + (char) c);
            }

            c = assertLineTerminates(c);
            c = skipLine(c);

            currentStatement = createStatement(subject, predicate, object, this.context);

            subject = null;
            predicate = null;
            object = null;
            this.context = null;

            return c;
        }
    }
}
