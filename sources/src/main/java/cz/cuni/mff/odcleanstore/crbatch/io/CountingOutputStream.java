/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A decorating output stream that counts the number of bytes that have passed
 * through the stream so far.
 * 
 * @author Jan Michelfeit
 */
public class CountingOutputStream extends FilterOutputStream {
    /** The count of bytes that have passed. */
    private long count = 0;
    
    private synchronized void addByteCount(int n) {
        count += n;
    }
    
    /**
     * The number of bytes that have passed through this stream.
     * @return the number of bytes accumulated
     */
    public synchronized long getByteCount() {
        return this.count;
    }
    
    /**
     * Creates a new counting output stream built on top of the specified underlying output stream.
     * @param out the underlying output stream 
     */
    public CountingOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(int b) throws IOException {
        addByteCount(1);
        out.write(b);
    }

    @Override
    public void write(byte[] bts) throws IOException {
        int len = bts != null ? bts.length : 0;
        addByteCount(len);
        out.write(bts);
    }

    @Override
    public void write(byte[] bts, int off, int len) throws IOException {
        addByteCount(len);
        out.write(bts, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            flush();
        } catch (IOException e) {
            // ignore
        }
        out.close();
    }
}
