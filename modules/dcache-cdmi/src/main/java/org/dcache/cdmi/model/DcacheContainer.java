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
import java.util.ListIterator;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.model.Container;

/* This class extends SNIA's Container class and implements/supports more metadata functions for dCache
 * than SNIA's CDMI reference implementation.
 */

public class DcacheContainer extends Container
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheContainer.class);

    // Container creation fields
    private Map<String, Object> exports = new HashMap<String, Object>();
    private String objectType;
    private String objectID;
    private String pnfsID;
    private String parentURI;
    private String domainURI;
    private String capabilitiesURI;
    private String completionStatus;
    private String childrenrange;
    private final List<String> children = new ArrayList<String>();

    private Map<String, String> metadata = new HashMap<String, String>();
    private List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();

    private final static List<String> IGNORE_LIST = new ArrayList() {{
        add("cdmi_ctime");
        add("cdmi_atime");
        add("cdmi_mtime");
        add("cdmi_size");
        add("cdmi_owner");
        add("cdmi_acl");
    }};

    @Override
    public Map<String, Object> getExports() {
        return exports;
    }

    @Override
    public String getObjectType()
    {
        return objectType;
    }

    @Override
    public String getCapabilitiesURI()
    {
        return capabilitiesURI;
    }

    @Override
    public String getParentURI()
    {
        return parentURI;
    }

    @Override
    public String getDomainURI()
    {
        return domainURI;
    }

    @Override
    public String getCompletionStatus()
    {
        return completionStatus;
    }

    @Override
    public String getChildrenrange()
    {
        return childrenrange;
    }

    @Override
    public List<String> getChildren()
    {
        return children;
    }

    @Override
    public Map<String, String> getMetadata()
    {
        return metadata;
    }

    public String getPnfsID()
    {
        return pnfsID;
    }

    @Override
    public void setObjectType(String objectType)
    {
        this.objectType = objectType;
    }

    @Override
    public void setCapabilitiesURI(String capabilitiesURI)
    {
        this.capabilitiesURI = capabilitiesURI;
    }

    @Override
    public void setParentURI(String parentURI)
    {
        this.parentURI = parentURI;
    }

    @Override
    public void setDomainURI(String domainURI)
    {
        this.domainURI = domainURI;
    }

    @Override
    public void setCompletionStatus(String completionStatus)
    {
        this.completionStatus = completionStatus;
    }

    @Override
    public void setChildrenrange(String childrenrange)
    {
        this.childrenrange = childrenrange;
    }

    public void setPnfsID(String pnfsId)
    {
        this.pnfsID = pnfsId;
    }

    public List<HashMap<String, String>> getSubMetadata_ACL()
    {
        return subMetadata_ACL;
    }

    public void addSubMetadata_ACL(HashMap<String, String> metadata)
    {
        subMetadata_ACL.add(metadata);
    }

    public void addSubMetadata_ACL(int position, HashMap<String, String> metadata)
    {
        subMetadata_ACL.add(position, metadata);
    }

    public void setMetadata(String key, String val)
    {
        metadata.put(key, val);
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
    public String toJson(boolean toFile)
    {
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
        } catch (IOException ex) {
            return ("Error: " + ex);
        }
        //
        return outBuffer.toString();
    }

    public String metadataToJson(boolean toFile)
    {
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
                if (!IGNORE_LIST.contains(entry.getKey())) {
                    g.writeStringField(entry.getKey(), entry.getValue());
                }
            }
            g.writeEndObject();
            g.writeEndObject();

            g.flush();
        } catch (IOException ex) {
            return("Error: " + ex);
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
        _log.trace("DcacheContainer<fromJson>:");
        JsonToken tolkein;
        tolkein = jp.nextToken();// START_OBJECT
        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
            String key = jp.getCurrentName();
            switch (key) {
                case "metadata":
                    tolkein = jp.nextToken();
                    while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                        key = jp.getCurrentName();
                        tolkein = jp.nextToken();
                        String value = jp.getText();
                        _log.trace("- Key={} : Value={}", key, value);
                        this.getMetadata().put(key, value);
                    }   break;
                case "exports":
                    tolkein = jp.nextToken();
                    while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                        key = jp.getCurrentName();
                        tolkein = jp.nextToken(); // Start
                        tolkein = jp.nextToken(); // End
                        this.getExports().put(key, null);
                    }// while
                    break;
                case "capabilitiesURI":
                    {
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.trace("Key={} : Val={}", key, value2);
                        this.setCapabilitiesURI(value2);
                        break;
                    }
                case "domainURI":
                    {
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.trace("Key={} : Val={}", key, value2);
                        this.setDomainURI(value2);
                        break;
                    }
                case "move":
                    {
                        jp.nextToken();
                        String value2 = jp.getText();
                        _log.trace("Key={} : Val={}", key, value2);
                        this.setMove(value2);
                        break;
                    }
                case "valuetransferencoding":
                    jp.nextToken();
                    // Ignore, it's from the Python CDMI test client
                    break;
                default:
                    if (fromFile) { // accept rest of key-values
                        switch (key) {
                            case "objectID":
                            {
                                jp.nextToken();
                                String value2 = jp.getText();
                                _log.trace("Key={} : Val={}", key, value2);
                                this.setObjectID(value2);
                                break;
                            }
                            case "pnfsID":
                            {
                                jp.nextToken();
                                String value2 = jp.getText();
                                _log.trace("Key={} : Val={}", key, value2);
                                this.setPnfsID(value2);
                                break;
                            }
                            default:
                                _log.trace("Invalid Key: {}", key);
                                throw new BadRequestException("Invalid Key: " + key);
                        }
                    } else {
                        _log.trace("Invalid Key: {}", key);
                        throw new BadRequestException("Invalid Key : " + key);
                    }
            }
        }
    }

}
