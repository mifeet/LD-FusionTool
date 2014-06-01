package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util;

import com.google.common.base.Supplier;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import java.util.HashMap;
import java.util.Map;

/**
 * Various utility methods for conflict resolution.
 */
public final class ODCSFusionToolCRUtils {
    //private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolCRUtils.class);

    /** Disable constructor for a utility class. */
    private ODCSFusionToolCRUtils() {
    }

    /**
     * Creates a new HashMap backed {@link Table}.
     */
    public static <R, C, V> Table<R, C, V> newHashTable() {
        return Tables.newCustomTable(
                new HashMap<R, Map<C, V>>(),
                new Supplier<Map<C, V>>() {
                    @Override
                    public Map<C, V> get() {
                        return new HashMap<>();
                    }
                }
        );
    }
}
