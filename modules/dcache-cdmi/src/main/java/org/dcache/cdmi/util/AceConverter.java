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
package org.dcache.cdmi.util;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.Who;
import org.dcache.cdmi.model.CdmiAce;

public class AceConverter
{
    public String convertToCdmiAceType(String value)
    {
        int hex = 0;
        if (value.equals(AceType.ACCESS_DENIED_ACE_TYPE.name()))
            hex = CdmiAce.CDMI_ACE_ACCESS_DENY;
        else if (value.equals(AceType.ACCESS_ALLOWED_ACE_TYPE.name()))
            hex = CdmiAce.CDMI_ACE_ACCESS_ALLOW;
        return String.format("0x%08X", (hex));
    }

    public String convertToCdmiAceWho(String value)
    {
        String result = "";
        if (value.equals(Who.OWNER.toString()))
            result = CdmiAce.OWNER;
        else if (value.equals(Who.GROUP.toString()))
            result = CdmiAce.GROUP;
        else if (value.equals(Who.EVERYONE.toString()))
            result = CdmiAce.EVERYONE;
        else if (value.equals(Who.ANONYMOUS.toString()))
            result = CdmiAce.ANONYMOUS;
        else if (value.equals(Who.AUTHENTICATED.toString()))
            result = CdmiAce.AUTHENTICATED;
        else //not CDMI standard
            result = (!value.endsWith("@")) ? value + "@" : value;
        return result;
    }

    public String convertToCdmiAceFlags(String value)
    {
        int hex = 0;
        char[] array = value.toCharArray();
        for (char elem : array) {
            if (elem == AceFlags.IDENTIFIER_GROUP.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_FLAGS_IDENTIFIER_GROUP;
            else if (elem == AceFlags.INHERIT_ONLY_ACE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_FLAGS_INHERIT_ONLY_ACE;
            else if (elem == AceFlags.DIRECTORY_INHERIT_ACE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE;
            else if (elem == AceFlags.FILE_INHERIT_ACE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE;
        }
        return String.format("0x%08X", (hex));
    }

    public String convertToCdmiAceMask(String value)
    {
        int hex = 0;
        char[] array = value.toCharArray();
        for (char elem : array) {
            if (elem == AccessMask.SYNCHRONIZE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_SYNCHRONIZE;
            else if (elem == AccessMask.WRITE_OWNER.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_WRITE_OWNER;
            else if (elem == AccessMask.WRITE_ACL.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_WRITE_ACL;
            else if (elem == AccessMask.READ_ACL.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_READ_ACL;
            else if (elem == AccessMask.DELETE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_DELETE;
            else if (elem == AccessMask.WRITE_ATTRIBUTES.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_WRITE_ATTRIBUTES;
            else if (elem == AccessMask.READ_ATTRIBUTES.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_READ_ATTRIBUTES;
            else if (elem == AccessMask.DELETE_CHILD.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_DELETE_SUBCONTAINER;
            else if (elem == AccessMask.DELETE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_DELETE_OBJECT;
            else if (elem == AccessMask.EXECUTE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_EXECUTE;
            else if (elem == AccessMask.WRITE_NAMED_ATTRS.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_WRITE_METADATA;
            else if (elem == AccessMask.READ_NAMED_ATTRS.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_READ_METADATA;
            else if (elem == AccessMask.ADD_SUBDIRECTORY.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_ADD_SUBCONTAINER;
            else if (elem == AccessMask.APPEND_DATA.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_APPEND_DATA;
            else if (elem == AccessMask.ADD_FILE.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_ADD_OBJECT;
            else if (elem == AccessMask.WRITE_DATA.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_WRITE_OBJECT;
            else if (elem == AccessMask.LIST_DIRECTORY.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_LIST_CONTAINER;
            else if (elem == AccessMask.READ_DATA.getAbbreviation())
                hex = hex | CdmiAce.CDMI_ACE_READ_OBJECT;
        }
        return String.format("0x%08X", (hex));
    }
}
