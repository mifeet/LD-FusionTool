package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import cz.cuni.mff.odcleanstore.fusiontool.util.Closeable;
import org.openrdf.model.Value;
import org.openrdf.rio.ntriples.NTriplesUtil;

import java.io.IOException;
import java.io.Writer;

/**
 * TODO
 */
public class NTuplesWriter implements Closeable<IOException> {
    protected final Writer writer;

    public NTuplesWriter(Writer outputWriter) {
        this.writer = outputWriter;
    }

    public void writeTuple(Value... values) throws IOException {
        if (values == null || values.length == 0) {
            return;
        }
        for (Value value : values) {
            NTriplesUtil.append(value, writer);
            writer.write(" ");
        }
        writer.write(".\n");
    }


    @Override
    public void close() throws IOException {
        writer.flush();
        writer.close();
    }
}