package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.conflictresolution.impl.SortedListModel;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.CRUtils;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.GrowingStatementArray;
import cz.cuni.mff.odcleanstore.conflictresolution.impl.util.SpogComparator;
import org.openrdf.model.Model;
import org.openrdf.model.Statement;

import java.util.*;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;

public class SortedListModelFactory {
    private static final Comparator<Statement> SPOG_COMPARATOR = new SpogComparator();

    public Model fromUnorderedList(Collection<Statement> statements) {
        Statement[] statementArray = statements.toArray(new Statement[statements.size()]);
        return fromArray(statementArray, statementArray.length);
    }

    public Model fromUnorderedIterator(Iterator<Statement> statements) {
        GrowingStatementArray growingArray = new GrowingStatementArray();
        while (statements.hasNext()) {
            growingArray.add(statements.next());
        }
        growingArray.getArray();
        return fromArray(growingArray.getArray(), growingArray.size());
    }

    private Model fromArray(Statement[] statementArray, int statementCount) {
        Arrays.sort(statementArray, 0, statementCount, SPOG_COMPARATOR);
        int newStatementCount = makeUnique(statementArray, statementCount);
        List<Statement> sortedStatementList = new ArrayList<>(statementArray, newStatementCount);
        return new SortedListModel(sortedStatementList);
    }

    /**
     * Removes identical quads.
     * Expects the quads to be spog-sorted in advance.
     * @return number of unique quads left in the array
     */
    private static int makeUnique(Statement[] statements, int originalSize) {
        if (originalSize == 0) {
            return 0;
        }

        int lastIdx = 0;
        for (int currIdx = 1; currIdx < originalSize; currIdx++) { // intentionally start from 1
            Statement previous = statements[lastIdx];
            Statement current = statements[currIdx];
            if (!CRUtils.statementsEqual(current, previous)) {
                lastIdx++;
                statements[lastIdx] = statements[currIdx];
            }
        }

        // Update size - we may use less of the underlying array now
        int newSize = lastIdx + 1;

        for (int i = newSize; i < originalSize; i++) {
            statements[i] = null; // release for GC
        }

        return newSize;
    }

    private static class ArrayList<E> extends AbstractList<E> implements RandomAccess, java.io.Serializable{
        private static final long serialVersionUID = -1246375464109943164L;
        private final E[] array;
        private final int size;

        ArrayList(E[] array, int size) {
            this.array = checkNotNull(array);
            this.size = size;
        }

        public int size() {
            return size;
        }

        public E get(int index) {
            return array[checkElementIndex(index, size)];
        }
    }
}
