package cz.cuni.mff.odcleanstore.fusiontool.loaders;

import cz.cuni.mff.odcleanstore.fusiontool.exceptions.NTupleMergeTransformException;
import cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesFileMerger;
import org.openrdf.model.Value;

import java.util.List;

/**
 * Merges records for primary temporary input file and attribute temporary file for
 * {@link cz.cuni.mff.odcleanstore.fusiontool.loaders.ExternalSortingInputLoader3}.
 * The expected format of files is:
 * <dl>
 *     <dt>primary input file</dt><dd>S P O G for input quads (S,P,O,G)</dd>
 *     <dt>attribute file</dt><dd>S E for input quads (E,P,S,G) such that P is a resource description URI</dd>
 * </dl>
 * The output is formatted as:
 * <dl><dt>output file</dt><dd>E S P O G for input quads (E,P',S,G') (S,P,O,G) such that P' is a resource description URI</dd></dl>
 */
public class InputFileAndAttributeFileMerger implements NTuplesFileMerger.NTupleMergeTransform {

    @Override
    public Value[] transform(List<Value> primaryInputFileValues, List<Value> attributeFileValues) throws NTupleMergeTransformException {
        // S P O G + S E -> E S P O G
        if (primaryInputFileValues.size() != 4) {
            throw new NTupleMergeTransformException("Unexpected format of input data in merge, expected 4 fields in primary input file.");
        } else if (attributeFileValues.size() != 2) {
            throw new NTupleMergeTransformException("Unexpected format of input data in merge, expected 2 fields in attribute file.");
        }

        Value[] result = new Value[5];
        result[0] = attributeFileValues.get(1);
        int i = 1;
        for (Value value : primaryInputFileValues) {
            result[i++] = value;
        }
        return result;
    }
}
