package common;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Utils
 * Created by breynard on 24/10/16.
 */
public class Utils {

    // Créer un fichier de données bidon (le lancer via les tests unitaires.
    public static String createFile() throws IOException {
        File tmpFile = File.createTempFile("dataLong", "100.txt");
        try (FileWriter sw = new FileWriter(tmpFile)) {
            for (int i = 0; i < 10000000; i++) {
                sw.write(i + "," + "Person_" + i+"\n");
            }
        }
        return tmpFile.getAbsolutePath();
    }

    public static void removeDirectory(File dir) {
        if (dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (File aFile : files) {
                    removeDirectory(aFile);
                }
            }
            dir.delete();
        } else {
            dir.delete();
        }
    }
}
