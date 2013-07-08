/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper factory class for collections which may not fit into memory and need to be backed by a file.
 * Current implementation uses MapDB (http://www.mapdb.org).
 * Note that calling size() on produced collections may not have constant time complexity!
 * @author Jan Michelfeit
 */
public class MapdbCollectionFactory implements LargeCollectionFactory {
    private static final Logger LOG = LoggerFactory.getLogger(MapdbCollectionFactory.class);
    
    private static final String TEMP_FILE_PREFIX = "odcs-fusiontool.db.";
    private static final String TEMP_FILE_SUFFIX = ".tmp";

    private final File dbFile;
    private DB db;
    private boolean isClosed = false;

    /**
     * Creates a new instance with a temporary file used to back contents of produced collections.
     * @param tempFileDirectory directory where the temporary file backing up produced collections will be created
     * @throws IOException error creating temporary file
     */
    public MapdbCollectionFactory(File tempFileDirectory) throws IOException {
        this.dbFile = createTempFile(tempFileDirectory);
    }

    /**
     * Creates a new instance with a unique temporary file used to back contents of produced collections.
     * @throws IOException error creating temporary file
     */
    public MapdbCollectionFactory() throws IOException {
        this(null);
    }
    
    private File createTempFile(File tempFileDirectory) throws IOException {
        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX, tempFileDirectory);
        LOG.debug("Creating temporary cache file {}", file.getName());
        return file;
    }

    @Override
    public <T> Set<T> createSet() {
        return createTempFileBackedSet(UUID.randomUUID().toString()); 
    }
    
    /**
     * Creates a new Set backed by the current temporary file.
     * If a set with the same name already exists in the current file, returns a reference to it.
     * @param name name of the collection
     * @param <T> type of collection
     * @return a new Set backed by a temporary file
     */
    public <T> Set<T> createTempFileBackedSet(String name) {
        return getDb().getTreeSet(name);
        // HashSet produces produces the following bug: 
        // isEmpty() reports true even though the collection contains several items
        // return getDb().getHashSet(name);
    }

    @Override
    public void close() throws IOException {
        if (db != null) {
            db.close();
            db = null;
        }
        isClosed = true;
    }
    
    private DB getDb() {
        if (isClosed) {
            throw new IllegalStateException(this.getClass().getSimpleName() + " has been closed");
        }
        if (db == null) {
            db = DBMaker.newFileDB(dbFile)
                    .deleteFilesAfterClose()
                    .closeOnJvmShutdown()
                    .writeAheadLogDisable()
                    .cacheSoftRefEnable()
                    .compressionEnable()
                    .make();
        }
        return db;
    }
}
