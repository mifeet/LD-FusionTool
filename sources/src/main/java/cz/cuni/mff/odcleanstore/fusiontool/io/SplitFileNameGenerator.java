/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.File;

/**
 * Generator of {@link File files} distinguished by suffixes. 
 * Useful when output is meant to be split across several files which differ only in the suffix.
 * The class is not thread-safe.
 * @author Jan Michelfeit
 */
public class SplitFileNameGenerator {
    /** Separator of file name and appended suffix. */
    public static final String SUFFIX_SEPARATOR = "-";
    private final File baseFileName;
    private int fileCount = 0;
    
    /**
     * @param baseFileName base file to which distinguishing suffixes will be appended
     */
    public SplitFileNameGenerator(File baseFileName) {
        this.baseFileName = baseFileName;
    }
    
    /**
     * Returns next file with suffix '-n' (if this is the n-th generated file name) appended to base file.
     * The suffix is appended before file extension if there is one.
     * @return file with numeric suffix appended to base file
     */
    public File nextFile() {
        return nextFile(Integer.toString(fileCount + 1));
    }
    
    /**
     * Returns next file with the given suffix appended to base file.
     * The suffix is appended before file extension if there is one.
     * @param suffix suffix to append to file name
     * @return file with the given suffix appended to base file
     */
    public File nextFile(String suffix) {
        fileCount++;
        return addFileNameSuffix(baseFileName, suffix);
    }
    
    private static File addFileNameSuffix(File file, String suffix) {
        String originalName = file.getName();
        String newName;
        int dotIndex = originalName.lastIndexOf('.');
        if (dotIndex < 0) {
            newName = originalName + SUFFIX_SEPARATOR + suffix;
        } else {
            newName = originalName.substring(0, dotIndex) + SUFFIX_SEPARATOR + suffix + originalName.substring(dotIndex);
        }
        return new File(file.getParentFile(), newName);
    }
}
