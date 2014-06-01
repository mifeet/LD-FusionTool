package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.util;

import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;
import org.openrdf.model.Statement;

import java.util.*;

/**
 * Various utility methods for conflict resolution.
 */
public final class ODCSFusionToolCRUtils {
    //private static final Logger LOG = LoggerFactory.getLogger(ODCSFusionToolCRUtils.class);
    private static final Supplier<List<Statement>> LIST_SUPPLIER = new ListSupplier();

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

    public static <T> ListMultimap<T, Statement> newStatementMultimap() {
        return Multimaps.newListMultimap(new HashMap<T, Collection<Statement>>(), LIST_SUPPLIER);
    }

    private static class ListSupplier implements Supplier<List<Statement>> {
        @Override
        public List<Statement> get() {
            return new ArrayList<>(5);
        }
    }
}
