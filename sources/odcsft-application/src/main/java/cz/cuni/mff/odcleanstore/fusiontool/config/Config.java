package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Encapsulation of ODCS-FusionTool configuration.
 * @author Jan Michelfeit
 */
public interface Config extends
        ConfigIO,
        ConfigData,
        ConfigProcessing,
        ConfigQuality,
        ConfigConflictResolution {
    // all members are inherited from parent interfaces
}