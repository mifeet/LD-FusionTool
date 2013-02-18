/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Method;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.xmloutput.impl.Basic;
import com.hp.hpl.jena.xmloutput.impl.JenaRdfXmlWriterTrojan;

/**
 * @author Jan Michelfeit
 */
public class IncrementalRdfXmlWriter extends JenaRdfXmlWriterTrojan implements CloseableRDFWriter {
    private String space;
    private String lastBase = "";
    private boolean isFirstWrite = true;
    private final PrintWriter outputWriter;
    private boolean closed = false;

    /**
     * @param outputWriter writer to which result is written
     */
    public IncrementalRdfXmlWriter(Writer outputWriter) {
        PrintWriter pw = outputWriter instanceof PrintWriter ? (PrintWriter) outputWriter : new PrintWriter(outputWriter);
        this.outputWriter = pw;
    }

    @Override
    public synchronized void write(Model baseModel, Writer outIgnored, String base) {
        // Avoid writing header declarations multiple times
        if (!isFirstWrite) {
            setProperty("showXmlDeclaration", false);
            setProperty("showDoctypeDeclaration", false);
        }

        // Keep blank node identifiers between calls
        setProperty("longid", true);

        super.write(baseModel, outputWriter, base);
        isFirstWrite = false;
    }

    @Override
    protected void writeBody(Model model, PrintWriter pwIgnored, String base, boolean inclXMLBase) {
        lastBase = base;
        setSpaceFromTabCount();
        String xmlns = jenaXmlnsDecl();
        if (isFirstWrite) {
            writeRDFHeader(model, outputWriter, xmlns);
        }
        writeRDFStatements(model, outputWriter);
    }

    private void setSpaceFromTabCount() {
        // Ugly hack, but invoking it on Basic class (from which we inherit) 
        // is necessary (that's the way Basic is written :/)
        
        
        try {
            Method parentMethod = Basic.class.getDeclaredMethod("setSpaceFromTabCount");
            parentMethod.setAccessible(true);
            parentMethod.invoke(this);
        } catch (Exception e) {
            throw new RuntimeException("Unable to call setSpaceFromTabCount() on parent", e);
        }
        
        space = "";
        for (int i = 0; i < getTabSize(); i += 1) {
            space += " ";
        }
    }
    
    @Override
    protected void writeSpace(PrintWriter writerIgnored) {
        outputWriter.print(space);
    }

    private void writeRDFHeader(Model model, PrintWriter writerIgnored, String xmlnsDecl) {
        outputWriter.print("<" + jenaRdfEl("RDF") + xmlnsDecl);
        String xmlBase = getXmlBase();
        if (null != xmlBase && xmlBase.length() > 0) {
            outputWriter.print("\n  xml:base=" + substitutedAttribute(xmlBase));
        }
        outputWriter.println(" > ");
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        writeRDFTrailer(outputWriter, lastBase);
        outputWriter.close();
        closed = true;
    }

    @Override
    public void write(Model model) {
        write(model, outputWriter, "");
    }

}
