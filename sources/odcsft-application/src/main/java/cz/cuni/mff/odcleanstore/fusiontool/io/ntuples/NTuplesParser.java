package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import cz.cuni.mff.odcleanstore.fusiontool.util.ThrowingAbstractIterator;
import org.openrdf.model.Value;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.helpers.NTriplesParserSettings;
import org.openrdf.rio.ntriples.NTriplesParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO
 */
public class NTuplesParser extends ThrowingAbstractIterator<List<Value>, IOException> implements Closeable<IOException> {
    private static final Logger LOG = LoggerFactory.getLogger(NTuplesParser.class);

    private Parser internalParser;

    public NTuplesParser(Reader inputReader, ParserConfig parserConfig) throws IOException {
        this.internalParser = new Parser(inputReader, parserConfig);
    }

    @Override
    protected List<Value> computeNext() throws IOException {
        List<Value> next = internalParser.parseNext();
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

    private static class Parser extends NTriplesParser implements Closeable<IOException> {
        private int expectedElements = 2;
        private int c;
        private List<Value> tuple;

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

        public List<Value> parseNext() throws IOException {
            tuple = null;
            while (c != -1 && tuple == null) {
                if (c == '#') {
                    c = skipLine(c); // Comment, ignore
                } else if (c == '\r' || c == '\n') {
                    c = skipLine(c); // Empty line, ignore
                } else {
                    try {
                        c = parseTuple(c);
                    } catch (RDFParseException e) {
                        String errorMessage = String.format("Parse error: %s before '%s'", e.getMessage(), readTillEndOfLine());
                        if (getParserConfig().isNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES)) {
                            tuple = null;
                            if (getParseErrorListener() != null) {
                                getParseErrorListener().error(errorMessage, e.getLineNumber(), e.getColumnNumber());
                            }
                            LOG.error(errorMessage, e);
                        } else {
                            throw new IOException(errorMessage, e);
                        }
                    }
                }

                c = skipWhitespace(c);
            }
            return tuple;
        }

        @Override
        public void close() throws IOException {
            clear();
            reader.close();
        }

        private int parseTuple(int c) throws IOException, RDFParseException {
            tuple = new ArrayList<>(expectedElements);
            while (c != -1 && c != '.' && c != '\r' && c != '\n') {
                c = parseObject(c);
                tuple.add(object);
                c = skipWhitespace(c);
            }

            if (c == -1) {
                throwEOFException();
            } else if (c != '.') {
                reportFatalError("Expected '.', found: " + (char) c);
            } else if (tuple.isEmpty()) {
                reportFatalError("Expected a value, found: " + (char) c);
            }

            c = assertLineTerminates(c);
            c = skipLine(c);
            if (tuple.size() > expectedElements) {
                expectedElements = tuple.size();
            }
            return c;
        }

        private String readTillEndOfLine() throws IOException {
            StringBuilder stringBuilder = new StringBuilder();
            if (c != -1 && c != '\r' && c != '\n') {
                c = reader.read();
            }
            while (c != -1 && c != '\r' && c != '\n') {
                stringBuilder.append((char)c);
                c = reader.read();
            }
            c = skipLine(c);
            return stringBuilder.toString();
        }
    }
}
