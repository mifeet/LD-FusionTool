package cz.cuni.mff.odcleanstore.fusiontool.loaders.extsort;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.NTupleMergeTransformException;
import cz.cuni.mff.odcleanstore.fusiontool.io.ntuples.NTuplesFileMerger;
import org.openrdf.model.Value;

import java.util.List;

/**
 * Merges records for primary data temporary file and attribute temporary file for
 * {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.ExternalSortingInputLoader}.
 * The expected format of files is
 * <dl>
 *     <dt>primary data file</dt><dd>c(S) S P O G for input quads (S,P,O,G)</dd>
 *     <dt>attribute file</dt><dd>c(S) c(E) for input quads (E,P,S,G) such that P is a resource description URI and S is a {@link org.openrdf.model.Resource}</dd>
 * </dl>
 * where c(x) is the canonical version of x.
 * The output is formatted as:
 * <dl><dt>output file</dt><dd>c(E) S P O G for input quads (E,P',S,G') (S,P,O,G) such that P' is a resource description URI</dd></dl>
 */
public class DataFileAndAttributeIndexFileMerger implements NTuplesFileMerger.NTupleMergeTransform {

    @Override
    public Value[] transform(List<Value> dataFileValues, List<Value> attributeFileValues) throws NTupleMergeTransformException {
        // c(S) S P O G + c(S) c(E) -> c(E) S P O G
        if (dataFileValues.size() != 5) {
            throw new NTupleMergeTransformException("Unexpected format of input data in merge, expected 5 fields in primary data file.");
        } else if (attributeFileValues.size() != 2) {
            throw new NTupleMergeTransformException("Unexpected format of input data in merge, expected 2 fields in attribute file.");
        }
        assert dataFileValues.get(0).equals(attributeFileValues.get(0));

        Value[] result = new Value[5];
        result[0] = attributeFileValues.get(1);
        for (int i = 1; i < dataFileValues.size(); i++) {
            result[i] = dataFileValues.get(i);
        }
        return result;
    }
}
