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
import java.io.StringWriter;
import java.util.ListIterator;
import java.util.Map;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.snia.cdmiserver.model.Capability;

/**
 * <p>
 * Representation of a CDMI <em>Capability</em>.
 * </p>
 */
public class DcacheCapability extends Capability
{
    private String childrenrange;
    private String objectName;
    //There is a typo in Capability.setObjectType() in SNIA's reference implementation which results that the value becomes null
    private String objectType;

    public String getChildrenrange()
    {
        return childrenrange;
    }

    public String getObjectName()
    {
        return objectName;
    }

    //There is a typo in Capability.setObjectType() in SNIA's reference implementation which results that the value becomes null
    @Override
    public String getObjectType()
    {
        return objectType;
    }

    public void setChildrenrange(String childrenrange)
    {
        this.childrenrange = childrenrange;
    }

    public void setObjectName(String objectName)
    {
        this.objectName = objectName;
    }

    //There is a typo in Capability.setObjectType() in SNIA's reference implementation which results that the value becomes null
    @Override
    public void setObjectType(String objectType) {
        this.objectType = objectType;
    }

    public String toJson()
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

            g.writeObjectFieldStart("capabilities");
            for (Map.Entry<String, String> entry : super.getMetadata().entrySet()) {
                g.writeStringField(entry.getKey(), entry.getValue());
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
}
