package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.conflictresolution.ResolvedStatement;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import org.openrdf.model.*;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

/**
 * Implementation of {@link CloseableRDFWriter} writing to a formatted HTML document.
 * @author Jan Michelfeit
 */
public class CloseableHtmlWriter implements CloseableRDFWriter {
    private static final String ENCODING = "UTF-8";
    private static final int MAX_LENGTH = 110;
    private static final double MID_QUALITY = 0.25d;
    private static final double MAX_QUALITY = 1d;
    
    /** Namespace prefix mappings. */
    private final Map<String, String> namespaceMapping = new HashMap<String, String>();
    
    private final Writer writer;
    private int statementCounter = 0;
    
    /**
     * @param outputWriter writer to the output file
     * @throws IOException I/O error
     */
    public CloseableHtmlWriter(Writer outputWriter) throws IOException {
        this.writer = outputWriter;
        namespaceMapping.put("http://www.w3.org/2001/XMLSchema#", "xsd");
        namespaceMapping.put("http://www.w3.org/2000/01/rdf-schema#",  "rdfs");
        namespaceMapping.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#",  "rdf");
        namespaceMapping.put("http://xmlns.com/foaf/0.1/", "foaf");
        namespaceMapping.put("http://www.w3.org/2003/01/geo/wgs84_pos#", "geo");
        namespaceMapping.put("http://www.w3.org/2002/07/owl#", "owl");
        writeHeader(writer);
    }

    @Override
    public void close() throws IOException {
        writerFooter(writer);
        writer.close();
    }

    @Override
    public void writeQuads(Iterator<Statement> quads) throws IOException {
        // ignore
    }

    @Override
    public void write(Statement quad) throws IOException {
        // ignore
    }

    @Override
    public void writeResolvedStatements(Iterator<ResolvedStatement> resolvedStatements) throws IOException {
        while (resolvedStatements.hasNext()) {
            write(resolvedStatements.next());
        } 
    }

    @Override
    public void write(ResolvedStatement resolvedStatement) throws IOException {
        writeOpeningTr(writer, ++this.statementCounter);
        writer.write("<td>");
        writeNode(writer, resolvedStatement.getStatement().getSubject());
        writer.write("</td><td>");
        writeNode(writer, resolvedStatement.getStatement().getPredicate());
        writer.write("</td><td>");
        writeNode(writer, resolvedStatement.getStatement().getObject());
        writer.write("</td><td style=\"background-color:");
        double quality = resolvedStatement.getQuality();
        if (quality < MID_QUALITY) {
            writer.write("rgba(255,0,0," + (1 - quality / MID_QUALITY) + ")");
        } else {
            writer.write("rgba(0,255,0," + ((quality - MID_QUALITY) / (MAX_QUALITY - MID_QUALITY)) + ")");
        }
        writer.write(";\">");
        writer.write(String.format(Locale.ROOT, "%.5f", quality));
        writer.write("</td><td>");
        boolean first = true;
        for (Resource sourceURI : resolvedStatement.getSourceGraphNames()) {
            if (!first) {
                writer.write(", ");
            }
            first = false;
            writeAbsoluteLink(writer,  sourceURI.stringValue(), getPrefixedURI(sourceURI.stringValue()));
        }
        writer.write("</td></tr>\n");
    }

    @Override
    public void addNamespace(String prefix, String uri) throws IOException {
        namespaceMapping.put(uri, prefix);
    }
    
    /**
     * Write start of the HTML document.
     * @param writer output writer
     * @throws IOException if an I/O error occurs
     */
    private void writeHeader(Writer writer) throws IOException {
        writer.write("<!DOCTYPE html>"
                + "\n<html lang=\"en\">" 
                + "\n<head>"
                + "\n <meta charset=\"" + ENCODING + "\" />"
                + "\n <style type=\"text/css\">" 
                + "\n   body {font-family: Verdana,Sans-Serif,Arial; font-size:13px;}"
                + "\n   th, td, table {border: 1px solid lightgray;}"
                + "\n   td, th {border-left-width: 0px; border-top-width: 0px}"
                + "\n   th {background-color: #49B7E0; color:white; padding: 5px 2px;}"
                + "\n   td {background-color: #F2F4F5; padding: 5px 2px; max-width: 30%; }"
                + "\n   tr.odd td {background-color: #FFFFFF; }"
                + "\n   a { text-decoration:none; }"
                + "\n   a:hover {text-decoration:underline; }"
                + "\n </style>"
                + "\n <title>ODCS-FusionTool results</title>"
                + "\n</head>\n<body>");
        writer.write(" <table border=\"1\" cellspacing=\"0\" cellpadding=\"2\">\n");
        writer.write("  <tr><th>Subject</th><th>Predicate</th><th>Object</th>"
                + "<th>Quality</th><th>Source named graphs</th></tr>\n");
    }
    

    /**
     * Write end of the HTML document.
     * @param writer output writer
     * @throws IOException if an I/O error occurs
     */
    private void writerFooter(Writer writer) throws IOException {
        writer.write(" </table>\n");
        writer.write("\n</body>\n</html>");
    }
    
    /**
     * Write a single node.
     * @param writer output writer
     * @param value RDF node
     * @throws IOException if an I/O error occurs
     */
    private void writeNode(Writer writer, Value value) throws IOException {
        if (value instanceof URI) {
            String text = getPrefixedURI(value.stringValue());
            writeAbsoluteLink(writer,  value.stringValue(), text);
        } else if (value instanceof Literal) {
            String text = formatLiteral((Literal) value);
            writer.write(text);
        } else if (value instanceof BNode) {
            writeAbsoluteLink(writer, "#", "_:" + value.stringValue());
        } else {
            writer.write(ODCSUtils.toStringNullProof(value));
        }
    }
    
    /**
     * Return uri with namespace shortened to prefix if possible.
     * @param uri uri to format
     * @return uri with namespace shortened to prefix if possible
     */
    protected String getPrefixedURI(String uri) {
        if (ODCSUtils.isNullOrEmpty(uri)) {
            return uri;
        }
        int namespacePartLength = Math.max(uri.lastIndexOf('/'), uri.lastIndexOf('#')) + 1; // use a simple heuristic
        String prefix = 0 < namespacePartLength && namespacePartLength < uri.length()
                ? namespaceMapping.get(uri.substring(0, namespacePartLength))
                : null;
        return (prefix != null)
                ? prefix + ":" + uri.substring(namespacePartLength)
                : uri;
    }

    /**
     * Format a literal value for output.
     * @param literalValue a literal node (literalNode.isLiteral() must return true!)
     * @return literal value formatted for output
     */
    private String formatLiteral(Literal literalValue) {
        StringBuilder result = new StringBuilder();
        String lang = literalValue.getLanguage();
        URI dtype = literalValue.getDatatype();
        String label = literalValue.getLabel();

        result.append('"');
        result.append(label);
        result.append('"');
        if (!ODCSUtils.isNullOrEmpty(lang)) {
            result.append("@").append(lang);
        }
        if (dtype != null) {
            result.append(" ^^").append(getPrefixedURI(dtype.stringValue()));
        }
        return result.toString();
    }

    /**
     * Write an absolute hyperlink.
     * @param writer output writer
     * @param uri URI of the hyperlink
     * @param text text of the hyperlink
     * @throws IOException if an I/O error occurs
     */
    private void writeAbsoluteLink(Writer writer, CharSequence uri, String text) throws IOException {
        if (text.length() > MAX_LENGTH) {
            text = text.substring(0, MAX_LENGTH) + "...";
        }
        writer.append("<a href=\"")
                .append(escapeHTML(uri))
                .append("\">")
                .append(text)
                .append("</a>");
    }
    
    /**
     * Return a text escaped for use in HTML.
     * @param text text to escape
     * @return escaped text
     */
    private String escapeHTML(CharSequence text) {
        return text.toString()
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;")
                .replace("'", "&#x27;")
                .replace("/", "&#x2F;");
    }
    
    /**
     * Write an opening &lt;tr&gt; tag with the correct background color.
     * @param writer writer
     * @param rowIndex row index
     * @throws IOException exception
     */
    private void writeOpeningTr(Writer writer, int rowIndex) throws IOException {
        writer.write("  <tr");
        if (rowIndex % 2 != 0) {
            writer.write(" class=\"odd\"");
        }
        writer.write(">");
    }
}