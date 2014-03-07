/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.cdmi.model;

import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.model.Container;

/**
 *
 * @author Jana
 */
public class CDMIContainer extends Container {

    // Container creation fields
    private Map<String, Object> exports = new HashMap<String, Object>();
    private String copy;
    private String move;
    private String reference;
    private String snapshot; // To create a snapshot via the "update" operation
    // Container representation fields
    private String objectType;
    private String objectID;
    private String pnfsID;
    private String parentURI;
    private String domainURI;
    private String capabilitiesURI;
    private String completionStatus;
    private Integer percentComplete; // FIXME - Specification says String but that does not make
                                     // sense
    private List<String> snapshots = new ArrayList<String>();
    private String childrenrange;
    private List<String> children = new ArrayList<String>();

    private Map<String, String> metadata = new HashMap<String, String>();
    private List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
    private final List<String> ignoreList = new ArrayList() {{
        add("cdmi_ctime");
        add("cdmi_atime");
        add("cdmi_mtime");
        add("cdmi_size");
    }};

    @Override
    public Map<String, Object> getExports() {
        return exports;
    }

    @Override
    public String getCopy() {
        return copy;
    }

    @Override
    public void setCopy(String copy) {
        this.copy = copy;
    }

    @Override
    public String getMove() {
        return move;
    }

    @Override
    public void setMove(String move) {
        this.move = move;
    }

    @Override
    public String getReference() {
        return reference;
    }

    @Override
    public void setReference(String reference) {
        this.reference = reference;
    }

    @Override
    public String getSnapshot() {
        return snapshot;
    }

    @Override
    public void setSnapshot(String snapshot) {
        this.snapshot = snapshot;
    }

    @Override
    public String getObjectType() {
        return objectType;
    }

    @Override
    public void setObjectType(String objectURI) {
        this.objectType = objectURI;
    }

    @Override
    public String getParentURI() {
        return parentURI;
    }

    @Override
    public void setParentURI(String parentURI) {
        this.parentURI = parentURI;
    }

    @Override
    public String getDomainURI() {
        return domainURI;
    }

    @Override
    public void setDomainURI(String domainURI) {
        this.domainURI = domainURI;
    }

    @Override
    public String getCapabilitiesURI() {
        return capabilitiesURI;
    }

    @Override
    public void setCapabilitiesURI(String capabilitiesURI) {
        this.capabilitiesURI = capabilitiesURI;
    }

    @Override
    public String getCompletionStatus() {
        return completionStatus;
    }

    @Override
    public void setCompletionStatus(String completionStatus) {
        this.completionStatus = completionStatus;
    }

    @Override
    public Integer getPercentComplete() {
        return percentComplete;
    }

    @Override
    public void setPercentComplete(Integer percentComplete) {
        this.percentComplete = percentComplete;
    }

    @Override
    public List<String> getSnapshots() {
        return snapshots;
    }

    @Override
    public String getChildrenrange() {
        return childrenrange;
    }

    @Override
    public void setChildrenrange(String childrenrange) {
        this.childrenrange = childrenrange;
    }

    @Override
    public List<String> getChildren() {
        return children;
    }

    @Override
    public String getObjectID() {
        return objectID;
    }

    @Override
    public void setObjectID(String objectID) {
        this.objectID = objectID;
    }

    public String getPnfsID() {
        return pnfsID;
    }

    public void setPnfsID(String pnfsId) {
        this.pnfsID = pnfsId;
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    public List<HashMap<String, String>> getSubMetadata_ACL() {
        return subMetadata_ACL;
    }

    public void setMetadata(String key, String val) {
        metadata.put(key, val);
    }

    public void addSubMetadata_ACL(HashMap<String, String> metadata) {
        subMetadata_ACL.add(metadata);
    }

    public void addSubMetadata_ACL(int position, HashMap<String, String> metadata) {
        subMetadata_ACL.add(position, metadata);
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public void setSubMetadata_ACL(List<HashMap<String, String>> metadata) {
        this.subMetadata_ACL = metadata;
    }

    private boolean isValidSubMetadata_ACL() {
        boolean result = true;
        if (!subMetadata_ACL.isEmpty()) {
            for (HashMap<String, String> entry : subMetadata_ACL) {
                if (!entry.containsKey("acetype") || !entry.containsKey("identifier") ||
                    !entry.containsKey("aceflags") || !entry.containsKey("acemask")) {
                    result = false;
                }
            }
        } else {
            result = false;
        }
        return result;
    }

    @Override
    public String toJson(boolean toFile) {
        //
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();
            g.writeStartObject();

            g.writeStringField("objectID", objectID);

            g.writeStringField("capabilitiesURI", capabilitiesURI);
            g.writeStringField("domainURI", domainURI);

            g.writeObjectFieldStart("exports");
            for (Map.Entry<String, Object> entry : exports.entrySet()) {
                g.writeObjectFieldStart(entry.getKey());
                g.writeEndObject();
            }
            g.writeEndObject();

            if (!toFile) {
                g.writeStringField("objectType", objectType);
                g.writeStringField("parentURI", parentURI);
                g.writeArrayFieldStart("children");
                ListIterator<String> it = children.listIterator();
                while (it.hasNext()) {
                    g.writeString((String) it.next());
                }
                g.writeEndArray();
                g.writeStringField("childrenrange", childrenrange);
                if (completionStatus != null)
                    g.writeStringField("completionStatus", completionStatus);
            }

            g.writeEndObject();
            g.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            return ("Error : " + ex);
        }
        //
        return outBuffer.toString();
    }

    public String toJsonWithMetadata(boolean toFile) {
        //
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();
            g.writeStartObject();

            g.writeStringField("objectID", objectID);

            g.writeStringField("capabilitiesURI", capabilitiesURI);
            g.writeStringField("domainURI", domainURI);

            g.writeObjectFieldStart("exports");
            for (Map.Entry<String, Object> entry : exports.entrySet()) {
                g.writeObjectFieldStart(entry.getKey());
                g.writeEndObject();
            }
            g.writeEndObject();
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                g.writeStringField(entry.getKey(), entry.getValue());
            }
            if (!subMetadata_ACL.isEmpty() && isValidSubMetadata_ACL()) {
                g.writeObjectFieldStart("cdmi_acl");
                for (HashMap<String, String> entry : subMetadata_ACL) {
                    g.writeStringField("acetype", entry.get("acetype"));
                    g.writeStringField("identifier", entry.get("identifier"));
                    g.writeStringField("aceflags", entry.get("aceflags"));
                    g.writeStringField("acemask", entry.get("acemask"));
                }
                g.writeEndObject();
            }
            g.writeEndObject();

            if (!toFile) {
                g.writeStringField("objectType", objectType);
                g.writeStringField("parentURI", parentURI);
                g.writeArrayFieldStart("children");
                ListIterator<String> it = children.listIterator();
                while (it.hasNext()) {
                    g.writeString((String) it.next());
                }
                g.writeEndArray();
                g.writeStringField("childrenrange", childrenrange);
                if (completionStatus != null)
                    g.writeStringField("completionStatus", completionStatus);
            }

            g.writeEndObject();
            g.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            return ("Error : " + ex);
        }
        //
        return outBuffer.toString();
    }

    public String metadataToJson(boolean toFile) {
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();

            g.writeStartObject();
            if (objectID != null)
                g.writeStringField("objectID", objectID);
            if (pnfsID != null)
                g.writeStringField("pnfsID", pnfsID);
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                if (!ignoreList.contains(entry.getKey())) {
                    g.writeStringField(entry.getKey(), entry.getValue());
                }
            }
            if (!subMetadata_ACL.isEmpty() && isValidSubMetadata_ACL()) {
                g.writeObjectFieldStart("cdmi_acl");
                for (HashMap<String, String> entry : subMetadata_ACL) {
                    g.writeStringField("acetype", entry.get("acetype"));
                    g.writeStringField("identifier", entry.get("identifier"));
                    g.writeStringField("aceflags", entry.get("aceflags"));
                    g.writeStringField("acemask", entry.get("acemask"));
                }
                g.writeEndObject();
            }
            g.writeEndObject();
            g.writeEndObject();

            g.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            return("Error : " + ex);
        }
        return outBuffer.toString();
    }

    @Override
    public void fromJson(InputStream jsonIs, boolean fromFile) throws Exception {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(jsonIs);
        fromJson(jp, fromFile);
    }

    @Override
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
            if ("metadata".equals(key)) {// process metadata
                tolkein = jp.nextToken();
                while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                    key = jp.getCurrentName();
                    if ("cdmi_acl".equals(key)) {// process metadata_ACL
                        tolkein = jp.nextToken();
                        HashMap<String, String> tmpMap = new HashMap<String, String>();
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            key = jp.getCurrentName();
                            tolkein = jp.nextToken();
                            String value = jp.getText();
                            System.out.println("   Key = " + key + " : Value = " + value);
                        }// while
                        addSubMetadata_ACL(tmpMap);
                    } else {
                        tolkein = jp.nextToken();
                        String value = jp.getText();
                        System.out.println("   Key = " + key + " : Value = " + value);
                        this.getMetadata().put(key, value);
                    }// while
                }
            } else if ("exports".equals(key)) {// process exports
                tolkein = jp.nextToken();
                while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                    key = jp.getCurrentName();
                    tolkein = jp.nextToken(); // Start
                    tolkein = jp.nextToken(); // End
                    this.getExports().put(key, null); // jp.nextToken();
                }// while
            } else if ("capabilitiesURI".equals(key)) {// process capabilitiesURI
                jp.nextToken();
                String value2 = jp.getText();
                System.out.println("Key : " + key + " Val : " + value2);
                this.setCapabilitiesURI(value2);
            } else if ("domainURI".equals(key)) {// process domainURI
                jp.nextToken();
                String value2 = jp.getText();
                System.out.println("Key : " + key + " Val : " + value2);
                this.setDomainURI(value2);
            } else if ("move".equals(key)) {// process move
                jp.nextToken();
                String value2 = jp.getText();
                System.out.println("Key : " + key + " Val : " + value2);
                this.setMove(value2);
            } else {
                if (fromFile) { // accept rest of key-values
                    if ("objectID".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        System.out.println("Key : " + key + " Val : " + value2);
                        this.setObjectID(value2);
                    } else if ("pnfsID".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        System.out.println("Key : " + key + " Val : " + value2);
                        this.setPnfsID(value2);
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

    /*
    private void fromJson(JsonParser jp, boolean fromFile) throws Exception {
        JsonToken tolkein;
        tolkein = jp.nextToken();// START_OBJECT
        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            if ("metadata".equals(key)) {// process metadata
                tolkein = jp.nextToken();
                while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                    key = jp.getCurrentName();
                    if ("cdmi_acl".equals(key)) {// process metadata_ACL
                        tolkein = jp.nextToken();
                        HashMap<String, String> tmpMap = new HashMap<String, String>();
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            key = jp.getCurrentName();
                            tolkein = jp.nextToken();
                            String value = jp.getText();
                            System.out.println("   Key = " + key + " : Value = " + value);
                        }// while
                        this.getSubMetadata_ACL().add(tmpMap);
                    } else {
                        tolkein = jp.nextToken();
                        String value = jp.getText();
                        System.out.println("   Key = " + key + " : Value = " + value);
                        this.getMetadata().put(key, value);
                    }// while
                }
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
    */

    public Object getObjectURI() {
        throw new UnsupportedOperationException("Not yet implemented");
    }

}
