package org.dcache.cdmi.temp;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    public static ByteBuffer stringToByteBuffer(String data) {
	return ByteBuffer.wrap(data.getBytes());
    }

    public static String byteBufferToString(ByteBuffer data) {
        String result = "";
        try {
            result = new String(data.array(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
        }
        return result;
    }

}
