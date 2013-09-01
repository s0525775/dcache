package org.dcache.cdmi;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Temp, for tests
 * @author Jana
 */
public class Test {

    static public void write(String file, String data) {
        try {
            SimpleDateFormat formatter = new SimpleDateFormat ("dd.MM.yyyy HH:mm:ss");
            Date currentTime = new Date();
            FileWriter fileWriter = new FileWriter(file, true);
            try (BufferedWriter bufferWriter = new BufferedWriter(fileWriter)) {
                bufferWriter.write("[" + formatter.format(currentTime) + "]-" + data + "\n");
            }
        } catch (IOException ex) { }
    }

}
