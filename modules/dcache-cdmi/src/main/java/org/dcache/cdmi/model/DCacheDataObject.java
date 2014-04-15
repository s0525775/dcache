package org.dcache.cdmi.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.model.DataObject;

public class DCacheDataObject extends DataObject
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DCacheDataObject.class);

    // DataObject creation fields
    private String mimetype;
    private String deserialize;
    private String serialize;
    private String copy;
    private String move;
    private String reference;
    private String value;
    // DataObject representation fields
    private String objectType;
    private String objectID;
    private String pnfsID;
    private String parentURI;
    private String accountURI;
    private String capabilitiesURI;
    private String completionStatus;
    private Integer percentComplete; // FIXME for SNIA - Specification says String but that does not make
                                     // sense (SNIA says Integer now!)
    private String valuerange;

    // Representation also includes "mimetype", "metadata", and "value" from creation fields
    //

    private Map<String, String> metadata = new HashMap<String, String>();
    private List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
    private final List<String> ignoreList = new ArrayList() {{
        add("cdmi_ctime");
        add("cdmi_atime");
        add("cdmi_mtime");
        add("cdmi_size");
        add("cdmi_owner");
    }};

    @Override
    public String getMimetype()
    {
        return mimetype;
    }

    @Override
    public void setMimetype(String mimetype)
    {
        this.mimetype = mimetype;
    }

    @Override
    public String getDeserialize()
    {
        return deserialize;
    }

    @Override
    public void setDeserialize(String deserialize)
    {
        this.deserialize = deserialize;
    }

    @Override
    public String getSerialize()
    {
        return serialize;
    }

    @Override
    public void setSerialize(String serialize)
    {
        this.serialize = serialize;
    }

    @Override
    public String getCopy()
    {
        return copy;
    }

    @Override
    public void setCopy(String copy)
    {
        this.copy = copy;
    }

    @Override
    public String getMove()
    {
        return move;
    }

    @Override
    public void setMove(String move)
    {
        this.move = move;
    }

    @Override
    public String getReference()
    {
        return reference;
    }

    @Override
    public void setReference(String reference)
    {
        this.reference = reference;
    }

    @Override
    public String getValue()
    {
        return value;
    }

    @Override
    public void setValue(String value)
    {
        this.value = value;
    }

    @Override
    public String getObjectType()
    {
        return objectType;
    }

    @Override
    public void setObjectType(String objectURI)
    {
        this.objectType = objectURI;
    }

    @Override
    public String getParentURI()
    {
        return parentURI;
    }

    @Override
    public void setParentURI(String parentURI)
    {
        this.parentURI = parentURI;
    }

    @Override
    public String getAccountURI()
    {
        return accountURI;
    }

    @Override
    public void setAccountURI(String accountURI)
    {
        this.accountURI = accountURI;
    }

    @Override
    public String getCapabilitiesURI()
    {
        return capabilitiesURI;
    }

    @Override
    public void setCapabilitiesURI(String capabilitiesURI)
    {
        this.capabilitiesURI = capabilitiesURI;
    }

    @Override
    public String getCompletionStatus()
    {
        return completionStatus;
    }

    @Override
    public void setCompletionStatus(String completionStatus)
    {
        this.completionStatus = completionStatus;
    }

    @Override
    public Integer getPercentComplete()
    {
        return percentComplete;
    }

    @Override
    public void setPercentComplete(Integer percentComplete)
    {
        this.percentComplete = percentComplete;
    }

    @Override
    public String getValuerange()
    {
        return valuerange;
    }

    @Override
    public void setValuerange(String valuerange)
    {
        this.valuerange = valuerange;
    }

    @Override
    public String getObjectID()
    {
        return objectID;
    }

    @Override
    public void setObjectID(String objectID)
    {
        this.objectID = objectID;
    }

    public String getPnfsID()
    {
        return pnfsID;
    }

    public void setPnfsID(String pnfsId)
    {
        this.pnfsID = pnfsId;
    }

    @Override
    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public List<HashMap<String, String>> getSubMetadata_ACL()
    {
        return subMetadata_ACL;
    }

    @Override
    public void setMetadata(String key, String val)
    {
        metadata.put(key, val);
    }

    public void addSubMetadata_ACL(HashMap<String, String> metadata)
    {
        subMetadata_ACL.add(metadata);
    }

    public void addSubMetadata_ACL(int mainKey, HashMap<String, String> metadata)
    {
        subMetadata_ACL.add(mainKey, metadata);
    }

    public void setMetadata(Map<String, String> metadata)
    {
        this.metadata = metadata;
    }

    public void setSubMetadata_ACL(List<HashMap<String, String>> metadata)
    {
        this.subMetadata_ACL = metadata;
    }

    private boolean isValidSubMetadata_ACL()
    {
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
    public String toJson() throws Exception
    {
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();

            g.writeStartObject();
            // get top level metadata
            if (objectType != null)
                g.writeStringField("objectType", objectType);
            if (capabilitiesURI != null)
                g.writeStringField("capabilitiesURI", capabilitiesURI);
            if (objectID != null)
                g.writeStringField("objectID", objectID);
            if (mimetype != null)
                g.writeStringField("mimetype", mimetype);
            //
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
            //
            g.writeEndObject();

            g.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
        }

        return outBuffer.toString();
    }

    @Override
    public String metadataToJson() throws Exception
    {
        StringWriter outBuffer = new StringWriter();
        try {
            JsonFactory f = new JsonFactory();
            JsonGenerator g = f.createJsonGenerator(outBuffer);
            g.useDefaultPrettyPrinter();

            g.writeStartObject();
            // get top level metadata
            if (objectID != null)
                g.writeStringField("objectID", objectID);
            if (pnfsID != null)
                g.writeStringField("pnfsID", pnfsID);
            //
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                if (!ignoreList.contains(entry.getKey())) {
                    g.writeStringField(entry.getKey(), entry.getValue());
                }
            }
            g.writeEndObject();
            //
            g.writeEndObject();

            g.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
            throw ex;
        }

        return outBuffer.toString();
    }

    @Override
    public void fromJson(InputStream jsonIs, boolean fromFile) throws Exception
    {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(jsonIs);
        fromJson(jp, fromFile);
    }

    @Override
    public void fromJson(byte[] jsonBytes, boolean fromFile) throws Exception
    {
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(jsonBytes);
        fromJson(jp, fromFile);
    }

    private void fromJson(JsonParser jp, boolean fromFile) throws Exception
    {
        _log.debug("   CDMIDataObject<fromJson>:");
        JsonToken tolkein;
        tolkein = jp.nextToken();// START_OBJECT
        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            if ("metadata".equals(key)) {// process metadata
                tolkein = jp.nextToken();
                while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                    key = jp.getCurrentName();
                    tolkein = jp.nextToken();
                    String value = jp.getText();
                    _log.debug("   Key = " + key + " : Value = " + value);
                    this.getMetadata().put(key, value);
                    // jp.nextToken();
                }
            } else if ("value".equals(key)) { // process value
                jp.nextToken();
                String value1 = jp.getText();
                _log.debug("Key : " + key + " Val : " + value1);
                this.setValue(value1);
            } else if ("mimetype".equals(key)) { // process mimetype
                jp.nextToken();
                String value2 = jp.getText();
                _log.debug("Key : " + key + " Val : " + value2);
                this.setMimetype(value2);
            } else {
                if (fromFile) { // accept rest of key-values
                    if ("objectType".equals(key)) {
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.debug("Key : " + key + " Val : " + value2);
                        this.setObjectType(value2);
                    } else if ("capabilitiesURI".equals(key)) {
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.debug("Key : " + key + " Val : " + value2);
                        this.setCapabilitiesURI(value2);
                    } else if ("objectID".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.debug("Key : " + key + " Val : " + value2);
                        this.setObjectID(value2);
                    } else if ("pnfsID".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.debug("Key : " + key + " Val : " + value2);
                        this.setPnfsID(value2);
                    } else if ("valueRange".equals(key)) { // process value
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.debug("Key : " + key + " Val : " + value2);
                        this.setValuerange(value2);
                    } else {
                        _log.debug("Invalid Key : " + key);
                        throw new BadRequestException("Invalid Key : " + key);
                    } // inner if
                } else {
                    _log.debug("Invalid Key : " + key);
                    throw new BadRequestException("Invalid Key : " + key);
                }
            }
        }
    }

}
