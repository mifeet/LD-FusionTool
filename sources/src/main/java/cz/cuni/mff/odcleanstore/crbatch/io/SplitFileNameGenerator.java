/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.File;

/**
 * TODO
 * @author Jan Michelfeit
 */
public class SplitFileNameGenerator {
    public static final String SUFFIX_SEPARATOR = "-";
    private final File baseFileName;
    private int fileCount = 0;
    
    public SplitFileNameGenerator(File baseFileName) {
        this.baseFileName = baseFileName;
    }
    
    public File nextFile() {
        return nextFile(Integer.toString(fileCount + 1));
    }
    
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
