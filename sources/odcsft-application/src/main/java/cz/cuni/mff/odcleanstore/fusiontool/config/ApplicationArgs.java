package cz.cuni.mff.odcleanstore.fusiontool.config;

/** Parsed command line arguments representation. */
public class ApplicationArgs {
    public enum VerboseLevel {
        NOT_VERBOSE,
        VERBOSE,
        VERY_VERBOSE
    }

    private final VerboseLevel verboseLevel;
    //private final boolean outputConflictsOnly;
    private final boolean outputMappedSubjectsOnly;
    private final boolean isProfilingOn;
    private final String configFilePath;

    public ApplicationArgs(VerboseLevel verboseLevel, boolean isProfilingOn, String configFilePath,
            boolean outputConflictsOnly, boolean outputMappedSubjectsOnly) {

        this.verboseLevel = verboseLevel;
        this.isProfilingOn = isProfilingOn;
        this.configFilePath = configFilePath;
        //this.outputConflictsOnly = outputConflictsOnly;
        this.outputMappedSubjectsOnly = outputMappedSubjectsOnly;
    }

    public VerboseLevel getVerboseLevel() {
        return verboseLevel;
    }

    public boolean isProfilingOn() {
        return isProfilingOn;
    }

    public String getConfigFilePath() {
        return configFilePath;
    }

    //public boolean getOutputConflictsOnly() {
    //    return outputConflictsOnly;
    //}

    public boolean getOutputMappedSubjectsOnly() {
        return outputMappedSubjectsOnly;
    }
}
