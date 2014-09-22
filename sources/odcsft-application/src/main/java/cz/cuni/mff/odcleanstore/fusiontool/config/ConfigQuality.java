package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Configuration related to quality calculation.
 */
public interface ConfigQuality {
    /**
     * Coefficient used in quality computation formula. Value N means that (N+1)
     * sources with score 1 that agree on the result will increase the result
     * quality to 1.
     * @return agree coefficient
     */
    Double getAgreeCoefficient();

    /**
     * Graph score used if none is given in the input.
     * @return default score
     */
    Double getScoreIfUnknown();

    /**
     * Weight of the publisher score.
     * @return publisher score weight
     */
    Double getPublisherScoreWeight();

    /**
     * Difference between two dates when their distance is equal to MAX_DISTANCE in seconds.
     * @return time interval in seconds
     */
    Long getMaxDateDifference();
}
