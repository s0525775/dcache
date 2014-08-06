/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.cdmi.model;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.model.DataObject;

/* This class extends SNIA's DataObject class and implements/supports more metadata functions for dCache
 * than SNIA's CDMI reference implementation.
 */

public class DcacheDataObject extends DataObject
{
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheDataObject.class);

    private String valueTransferEncoding;
    private String domainURI;
    private String parentID;
    private List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();

    private final static List<String> IGNORE_LIST = new ArrayList() {{
        add("cdmi_ctime");
        add("cdmi_atime");
        add("cdmi_mtime");
        add("cdmi_size");
        add("cdmi_owner");
    }};

    public String getValueTransferEncoding()
    {
        return valueTransferEncoding;
    }

    public String getDomainURI()
    {
        return domainURI;
    }

    public String getParentID()
    {
        return parentID;
    }

    public List<HashMap<String, String>> getSubMetadata_ACL()
    {
        return subMetadata_ACL;
    }

    public void setValueTransferEncoding(String valueTransferEncoding)
    {
        this.valueTransferEncoding = valueTransferEncoding;
    }

    public void setDomainURI(String domainURI)
    {
        this.domainURI = domainURI;
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
        getMetadata().clear();
        for (Entry<String, String> entry : metadata.entrySet()) {
            setMetadata(entry.getKey(), entry.getValue());
        }
    }

    public void setSubMetadata_ACL(List<HashMap<String, String>> metadata)
    {
        this.subMetadata_ACL = metadata;
    }

    public void setParentID(String id)
    {
        parentID = id;
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
            if (getObjectType() != null)
                g.writeStringField("objectType", getObjectType());
            if (getCapabilitiesURI() != null)
                g.writeStringField("capabilitiesURI", getCapabilitiesURI());
            if (getObjectID() != null)
                g.writeStringField("objectID", getObjectID());
            if (getObjectID() != null)
                g.writeStringField("parentID", getParentID());
            if (getObjectID() != null)
                g.writeStringField("parentURI", getParentURI());
            if (getMimetype() != null)
                g.writeStringField("mimetype", getMimetype());
            if (getCompletionStatus() != null)
                g.writeStringField("completionStatus", getCompletionStatus());
            //
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
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
            if (getObjectID() != null)
                g.writeStringField("objectID", getObjectID());
            //
            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : getMetadata().entrySet()) {
                if (!IGNORE_LIST.contains(entry.getKey())) {
                    g.writeStringField(entry.getKey(), entry.getValue());
                }
            }
            g.writeEndObject();
            //
            g.writeEndObject();

            g.flush();
        } catch (IOException ex) {
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
        _log.trace("DcacheDataObject<fromJson>:");
        JsonToken tolkein;
        tolkein = jp.nextToken();// START_OBJECT
        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            switch (key) {
                case "metadata":
                    {
                        tolkein = jp.nextToken();
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            key = jp.getCurrentName();
                            tolkein = jp.nextToken();
                            String value = jp.getText();
                            _log.trace("- Key={} : Value={}", key, value);
                            this.getMetadata().put(key, value);
                        }//while
                        break;
                    }
                case "value":
                    {
                        jp.nextToken();
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        this.setValue(value);
                        break;
                    }
                case "mimetype":
                    {
                        jp.nextToken();
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        this.setMimetype(value);
                        break;
                    }
                case "move":
                    {
                        jp.nextToken();
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        super.setMove(value);
                        setMove(value);
                        break;
                    }
                case "domainURI":
                    {
                        jp.nextToken();
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        setDomainURI(value);
                        break;
                    }
                case "deserialize":
                    {
                        //not supported yet, but process it to prevent an error message
                        jp.nextToken();
                        break;
                    }
                case "serialize":
                    {
                        //not supported yet, but process it to prevent an error message
                        jp.nextToken();
                        break;
                    }
                case "copy":
                    {
                        //not supported yet, but process it to prevent an error message
                        jp.nextToken();
                        break;
                    }
                case "reference":
                    {
                        //not supported yet, but process it to prevent an error message
                        jp.nextToken();
                        break;
                    }
                case "deserializevalue":
                    {
                        //not supported yet, but process it to prevent an error message
                        jp.nextToken();
                        break;
                    }
                case "valuetransferencoding":
                    {
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        setValueTransferEncoding(value);
                        jp.nextToken();
                        break;
                    }
                default:
                    if (fromFile) { // accept rest of key-values
                        switch (key) {
                            case "objectType":
                                {
                                    jp.nextToken();
                                    String value = jp.getText();
                                    _log.trace("Key={} : Val={}", key, value);
                                    setObjectType(value);
                                    break;
                                }
                            case "capabilitiesURI":
                                {
                                    jp.nextToken();
                                    String value = jp.getText();
                                    _log.trace("Key={} : Val={}", key, value);
                                    setCapabilitiesURI(value);
                                    break;
                                }
                            case "objectID":
                                {
                                    jp.nextToken();
                                    String value = jp.getText();
                                    _log.trace("Key={} : Val={}", key, value);
                                    setObjectID(value);
                                    break;
                                }
                            case "valueRange":
                                {
                                    jp.nextToken();
                                    String value = jp.getText();
                                    _log.trace("Key={} : Val={}", key, value);
                                    setValuerange(value);
                                    break;
                                }
                            default:
                                {
                                    _log.trace("Invalid Key: {}", key);
                                    throw new BadRequestException("Invalid Key: " + key);
                                }
                        }
                    } else {
                        _log.trace("Invalid Key: {}", key);
                        throw new BadRequestException("Invalid Key: " + key);
                    }
            }
        }
    }

}
