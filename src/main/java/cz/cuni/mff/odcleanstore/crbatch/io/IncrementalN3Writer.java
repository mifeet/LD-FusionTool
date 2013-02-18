/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.Writer;
import java.util.Map;

import com.hp.hpl.jena.n3.N3JenaWriterCommon;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * @author Jan Michelfeit
 */
public class IncrementalN3Writer extends N3JenaWriterCommon implements CloseableRDFWriter {
    private int cachedBNodeCounter = -1;
    private Map<Resource, String> cachedBNodesMap = null;
    private final Writer outputWriter;

    /**
     * @param outputWriter writer to which result is written
     */
    public IncrementalN3Writer(Writer outputWriter) {
        this.outputWriter = outputWriter;
    }

    @Override
    protected void startWriting() {
        super.startWriting();
        alwaysAllocateBNodeLabel = true;
        if (cachedBNodesMap != null) {
            bNodesMap = cachedBNodesMap;
        }
        if (cachedBNodeCounter >= 0) {
            bNodeCounter = cachedBNodeCounter;
        }
    }

    @Override
    protected void finishWriting() {
        super.finishWriting();
        cachedBNodeCounter = bNodeCounter;
        cachedBNodesMap = bNodesMap;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void write(Model model) {
        write(model, outputWriter, "");
    }
}
