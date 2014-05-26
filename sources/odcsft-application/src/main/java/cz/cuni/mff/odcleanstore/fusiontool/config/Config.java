package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Encapsulation of ODCS-FusionTool configuration.
 * @author Jan Michelfeit
 */
public interface Config extends ConfigIO, ConfigProcessing, ConfigQuality, ConfigConflictResolution {
    // all members are divided in parent interfaces
}