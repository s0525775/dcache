/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.cdmi.dao.mongodb;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.snia.cdmiserver.exception.BadRequestException;

/**
 *
 * @author Jana
 */
public class DataObjectMetadata {

    private String objectID;
    private Map<String, String> metadata = new HashMap<String, String>();

    public String getObjectID() {
        return objectID;
    }

    public void setObjectID(String objectID) {
        this.objectID = objectID;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(String key, String val) {
        metadata.put(key, val);
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public String metadataToJson() throws Exception {
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();

            g.writeStartObject();
            if (objectID != null)
                g.writeStringField("objectID", objectID);
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                g.writeStringField(entry.getKey(), entry.getValue());
            }
            g.writeEndObject();
            g.writeEndObject();

            g.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
            //return("Error : " + ex);
        }

        return outBuffer.toString();
    }

    public void fromJson(InputStream jsonIs, boolean fromFile) throws Exception {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(jsonIs);
        fromJson(jp, fromFile);
    }

    public void fromJson(byte[] jsonBytes, boolean fromFile) throws Exception {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(jsonBytes);
        fromJson(jp, fromFile);
    }

    private void fromJson(JsonParser jp, boolean fromFile) throws Exception {
        JsonToken tolkein;
        tolkein = jp.nextToken();// START_OBJECT
        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            System.out.println("TEST999: " + key);
            if ("metadata".equals(key)) {// process metadata
                tolkein = jp.nextToken();
                while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                    key = jp.getCurrentName();
                    tolkein = jp.nextToken();
                    String value = jp.getText();
                    System.out.println("   Key = " + key + " : Value = " + value);
                    this.setMetadata(key, value);
                }// while
            } else {
                if (fromFile) { // accept rest of key-values
                    if ("objectID".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        System.out.println("Key : " + key + " Val : " + value2);
                        this.setObjectID(value2);
                    } else if ("_id".equals(key)) {
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            // that's a MongoDB feature, so ignore or you will throw an exception
                        }// while
                    } else {
                        System.out.println("Invalid Key : " + key);
                        throw new BadRequestException("Invalid Key : " + key);
                    } // inner if
                } else {
                    System.out.println("Invalid Key : " + key);
                    throw new BadRequestException("Invalid Key : " + key);
                }
            }
        }
    }

}
