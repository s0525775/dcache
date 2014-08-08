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
import java.util.Map.Entry;
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

    private String parentID;
    private String objectName;
    private List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();

    public void setChildren(List<String> children)
    {
        getChildren().clear();
        for (String child : children) {
            getChildren().add(child);
        }
    }

    public List<HashMap<String, String>> getSubMetadata_ACL()
    {
        return subMetadata_ACL;
    }

    public String getParentID()
    {
        return parentID;
    }

    public String getObjectName()
    {
        return objectName;
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
        getMetadata().put(key, val);
    }

    public void setParentID(String id)
    {
        parentID = id;
    }

    public void setObjectName(String name)
    {
        objectName = name;
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
            g.writeStringField("objectType", getObjectType());
            g.writeStringField("objectID", getObjectID());
            g.writeStringField("objectName", getObjectName());
            g.writeStringField("parentURI", getParentURI());
            g.writeStringField("parentID", getParentID());
            g.writeStringField("domainURI", getDomainURI());
            g.writeStringField("capabilitiesURI", getCapabilitiesURI());
            if (getCompletionStatus() != null)
                g.writeStringField("completionStatus", getCompletionStatus());
            g.writeObjectFieldStart("exports");
            for (Map.Entry<String, Object> entry : getExports().entrySet()) {
                g.writeObjectFieldStart(entry.getKey());
                g.writeEndObject();
            }
            g.writeEndObject();
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
            g.writeArrayFieldStart("children");
            ListIterator<String> it = getChildren().listIterator();
            while (it.hasNext()) {
                g.writeString((String) it.next());
            }
            g.writeEndArray();
            g.writeStringField("childrenrange", getChildrenrange());
            g.writeEndObject();
            g.flush();
        } catch (IOException ex) {
            return ("Error: " + ex);
        }
        //
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
                    {
                        tolkein = jp.nextToken();
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            key = jp.getCurrentName();
                            tolkein = jp.nextToken();
                            String value = jp.getText();
                            _log.trace("- Key={} : Value={}", key, value);
                            getMetadata().put(key, value);
                        }   break;
                    }
                case "exports":
                    {
                        tolkein = jp.nextToken();
                        while ((tolkein = jp.nextToken()) != JsonToken.END_OBJECT) {
                            key = jp.getCurrentName();
                            tolkein = jp.nextToken(); // Start
                            tolkein = jp.nextToken(); // End
                            getExports().put(key, null);
                        }// while
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
                case "domainURI":
                    {
                        jp.nextToken();
                        String value = jp.getText();
                        _log.trace("Key={} : Val={}", key, value);
                        setDomainURI(value);
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
                case "valuetransferencoding":
                    {
                        //Ignore
                        jp.nextToken();
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
                default:
                    {
                        _log.trace("Invalid Key: {}", key);
                        throw new BadRequestException("Invalid Key: " + key);
                    }
            }
        }
    }

}
