package cz.cuni.mff.odcleanstore.fusiontool.exceptions;

/**
* Exception thrown by {@link cz.cuni.mff.odcleanstore.fusiontool.io.NTuplesFileMerger.NTupleMergeTransform}.
*/
public class NTupleMergeTransformException extends Exception  {
    /**
     * Constructs a new exception with the given cause.
     * @param cause the cause
     */
    public NTupleMergeTransformException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a new exception with the given message and cause.
     * @param message the detail message
     * @param cause the cause
     */
    public NTupleMergeTransformException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the given message.
     * @param message the detail message
     */
    public NTupleMergeTransformException(String message) {
        super(message);
    }
}
