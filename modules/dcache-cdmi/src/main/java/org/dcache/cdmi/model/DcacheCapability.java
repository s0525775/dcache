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
import java.util.HashMap;
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

    public String getChildrenrange()
    {
        return childrenrange;
    }

    public void setChildrenrange(String childrenrange)
    {
        this.childrenrange = childrenrange;
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

            g.writeStringField("objectID", getObjectID());
            g.writeStringField("objectType", getObjectType());
            g.writeStringField("parentURI", getParentURI());
            g.writeStringField("capabilities", getCapabilities());

            g.writeObjectFieldStart("metadata");
            for (Map.Entry<String, String> entry : super.getMetadata().entrySet()) {
                g.writeStringField(entry.getKey(), entry.getValue());
            }
            g.writeEndObject();

            g.writeStringField("objectType", getObjectType());
            g.writeStringField("parentID", getParentID());
            g.writeStringField("parentURI", getParentURI());
            g.writeArrayFieldStart("children");
            ListIterator<String> it = getChildren().listIterator();
            while (it.hasNext()) {
                g.writeString((String) it.next());
            }
            g.writeEndArray();
            g.writeStringField("childrenrange", getChildrenrange());
            g.flush();
        } catch (IOException ex) {
            return ("Error: " + ex);
        }
        //
        return outBuffer.toString();
    }
}
