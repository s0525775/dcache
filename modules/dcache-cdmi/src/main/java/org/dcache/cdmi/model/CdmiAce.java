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

public class CdmiAce
{
    //ACE Types
    public final static int CDMI_ACE_ACCESS_ALLOW = 0x00000000;  //ACCESS_ALLOWED_ACE_TYPE
    public final static int CDMI_ACE_ACCESS_DENY = 0x00000001;  //ACCESS_DENIED_ACE_TYPE
    public final static int CDMI_ACE_SYSTEM_AUDIT = 0x00000002;
    public final static String STR_CDMI_ACE_ACCESS_ALLOW = "ALLOW";
    public final static String STR_CDMI_ACE_ACCESS_DENY = "DENY";
    public final static String STR_CDMI_ACE_SYSTEM_AUDIT = "AUDIT";
    //WHO Identifiers
    public final static String OWNER = "OWNER@";  //OWNER@
    public final static String GROUP = "GROUP@";  //GROUP@
    public final static String EVERYONE = "EVERYONE@";  //EVERYONE@
    public final static String ANONYMOUS = "ANONYMOUS@";  //ANONYMOUS@
    public final static String AUTHENTICATED = "AUTHENTICATED@";  //AUTHENTICATED@
    public final static String ADMINISTATOR = "ADMINISTRATOR@";
    public final static String ADMINUSERS = "ADMINUSERS@";
    //ACE Flags
    public final static int CDMI_ACE_FLAGS_NONE = 0x00000000;
    public final static int CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE = 0x00000001;  //f
    public final static int CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE = 0x00000002;  //d
    public final static int CDMI_ACE_FLAGS_NO_PROPAGATE_ACE = 0x00000004;
    public final static int CDMI_ACE_FLAGS_INHERIT_ONLY_ACE = 0x00000008;  //o
    public final static int CDMI_ACE_FLAGS_IDENTIFIER_GROUP = 0x00000040;  //exists
    public final static int CDMI_ACE_FLAGS_INHERITED_ACE = 0x00000080;
    public final static String STR_CDMI_ACE_FLAGS_NONE = "NO_FLAGS";
    public final static String STR_CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE = "OBJECT_INHERIT";
    public final static String STR_CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE = "CONTAINER_INHERIT";
    public final static String STR_CDMI_ACE_FLAGS_NO_PROPAGATE_ACE = "NO_PROPAGATE";
    public final static String STR_CDMI_ACE_FLAGS_INHERIT_ONLY_ACE = "INHERIT_ONLY";
    public final static String STR_CDMI_ACE_FLAGS_IDENTIFIER_GROUP = "IDENTIFIER_GROUP";
    public final static String STR_CDMI_ACE_FLAGS_INHERITED_ACE = "INHERITED";
    //ACE Bit Masks
    public final static int CDMI_ACE_READ_OBJECT = 0x00000001;  //r
    public final static int CDMI_ACE_LIST_CONTAINER = 0x00000001;  //l
    public final static int CDMI_ACE_WRITE_OBJECT = 0x00000002;  //w
    public final static int CDMI_ACE_ADD_OBJECT = 0x00000002;  //f
    public final static int CDMI_ACE_APPEND_DATA = 0x00000004;  //a
    public final static int CDMI_ACE_ADD_SUBCONTAINER = 0x00000004;  //s
    public final static int CDMI_ACE_READ_METADATA = 0x00000008;  //n
    public final static int CDMI_ACE_WRITE_METADATA = 0x00000010;  //N
    public final static int CDMI_ACE_EXECUTE = 0x00000020;  //x
    public final static int CDMI_ACE_DELETE_OBJECT = 0x00000040;  //d
    public final static int CDMI_ACE_DELETE_SUBCONTAINER = 0x00000040;  //d
    public final static int CDMI_ACE_READ_ATTRIBUTES = 0x00000080;  //t
    public final static int CDMI_ACE_WRITE_ATTRIBUTES = 0x00000100;  //T
    public final static int CDMI_ACE_WRITE_RETENTION = 0x00000200;
    public final static int CDMI_ACE_WRITE_RETENTION_HOLD = 0x00000400;
    public final static int CDMI_ACE_DELETE = 0x00010000;  //d
    public final static int CDMI_ACE_READ_ACL = 0x00020000;  //c
    public final static int CDMI_ACE_WRITE_ACL = 0x00040000;  //C
    public final static int CDMI_ACE_WRITE_OWNER = 0x00080000;  //o
    public final static int CDMI_ACE_SYNCHRONIZE = 0x00100000;  //exists
    public final static String STR_CDMI_ACE_READ_OBJECT = "READ_OBJECT";
    public final static String STR_CDMI_ACE_LIST_CONTAINER = "LIST_CONTAINER";
    public final static String STR_CDMI_ACE_WRITE_OBJECT = "WRITE_OBJECT";
    public final static String STR_CDMI_ACE_ADD_OBJECT = "ADD_OBJECT";
    public final static String STR_CDMI_ACE_APPEND_DATA = "APPEND_DATA";
    public final static String STR_CDMI_ACE_ADD_SUBCONTAINER = "ADD_SUBCONTAINER";
    public final static String STR_CDMI_ACE_READ_METADATA = "READ_METADATA";
    public final static String STR_CDMI_ACE_WRITE_METADATA = "WRITE_METADATA";
    public final static String STR_CDMI_ACE_EXECUTE = "EXECUTE";
    public final static String STR_CDMI_ACE_DELETE_OBJECT = "DELETE_OBJECT";
    public final static String STR_CDMI_ACE_DELETE_SUBCONTAINER = "DELETE_SUBCONTAINER";
    public final static String STR_CDMI_ACE_READ_ATTRIBUTES = "READ_ATTRIBUTES";
    public final static String STR_CDMI_ACE_WRITE_ATTRIBUTES = "WRITE_ATTRIBUTES";
    public final static String STR_CDMI_ACE_WRITE_RETENTION = "WRITE_RETENTION";
    public final static String STR_CDMI_ACE_WRITE_RETENTION_HOLD = "WRITE_RETENTION_HOLD";
    public final static String STR_CDMI_ACE_DELETE = "DELETE";
    public final static String STR_CDMI_ACE_READ_ACL = "READ_ACL";
    public final static String STR_CDMI_ACE_WRITE_ACL = "WRITE_ACL";
    public final static String STR_CDMI_ACE_WRITE_OWNER = "WRITE_OWNER";
    public final static String STR_CDMI_ACE_SYNCHRONIZE = "SYNCHRONIZE";

    public String toString(int value)
    {
        String result = "";
        //switch can't be used here because some values repeat
        if (value == CDMI_ACE_ACCESS_ALLOW)
            result = STR_CDMI_ACE_ACCESS_ALLOW;
        else if (value == CDMI_ACE_ACCESS_DENY)
            result = STR_CDMI_ACE_ACCESS_DENY;
        else if (value == CDMI_ACE_SYSTEM_AUDIT)
            result = STR_CDMI_ACE_SYSTEM_AUDIT;
        else if (value == CDMI_ACE_FLAGS_NONE)
            result = STR_CDMI_ACE_FLAGS_NONE;
        else if (value == CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE)
            result = STR_CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE;
        else if (value == CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE)
            result = STR_CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE;
        else if (value == CDMI_ACE_FLAGS_NO_PROPAGATE_ACE)
            result = STR_CDMI_ACE_FLAGS_NO_PROPAGATE_ACE;
        else if (value == CDMI_ACE_FLAGS_INHERIT_ONLY_ACE)
            result = STR_CDMI_ACE_FLAGS_INHERIT_ONLY_ACE;
        else if (value == CDMI_ACE_FLAGS_IDENTIFIER_GROUP)
            result = STR_CDMI_ACE_FLAGS_IDENTIFIER_GROUP;
        else if (value == CDMI_ACE_FLAGS_INHERITED_ACE)
            result = STR_CDMI_ACE_FLAGS_INHERITED_ACE;
        else if (value == CDMI_ACE_READ_OBJECT)
            result = STR_CDMI_ACE_READ_OBJECT;
        else if (value == CDMI_ACE_LIST_CONTAINER)
            result = STR_CDMI_ACE_LIST_CONTAINER;
        else if (value == CDMI_ACE_WRITE_OBJECT)
            result = STR_CDMI_ACE_WRITE_OBJECT;
        else if (value == CDMI_ACE_ADD_OBJECT)
            result = STR_CDMI_ACE_ADD_OBJECT;
        else if (value == CDMI_ACE_APPEND_DATA)
            result = STR_CDMI_ACE_APPEND_DATA;
        else if (value == CDMI_ACE_ADD_SUBCONTAINER)
            result = STR_CDMI_ACE_ADD_SUBCONTAINER;
        else if (value == CDMI_ACE_READ_METADATA)
            result = STR_CDMI_ACE_READ_METADATA;
        else if (value == CDMI_ACE_WRITE_METADATA)
            result = STR_CDMI_ACE_WRITE_METADATA;
        else if (value == CDMI_ACE_EXECUTE)
            result = STR_CDMI_ACE_EXECUTE;
        else if (value == CDMI_ACE_DELETE_OBJECT)
            result = STR_CDMI_ACE_DELETE_OBJECT;
        else if (value == CDMI_ACE_DELETE_SUBCONTAINER)
            result = STR_CDMI_ACE_DELETE_SUBCONTAINER;
        else if (value == CDMI_ACE_READ_ATTRIBUTES)
            result = STR_CDMI_ACE_READ_ATTRIBUTES;
        else if (value == CDMI_ACE_WRITE_ATTRIBUTES)
            result = STR_CDMI_ACE_WRITE_ATTRIBUTES;
        else if (value == CDMI_ACE_WRITE_RETENTION)
            result = STR_CDMI_ACE_WRITE_RETENTION;
        else if (value == CDMI_ACE_WRITE_RETENTION_HOLD)
            result = STR_CDMI_ACE_WRITE_RETENTION_HOLD;
        else if (value == CDMI_ACE_DELETE)
            result = STR_CDMI_ACE_DELETE;
        else if (value == CDMI_ACE_READ_ACL)
            result = STR_CDMI_ACE_READ_ACL;
        else if (value == CDMI_ACE_WRITE_ACL)
            result = STR_CDMI_ACE_WRITE_ACL;
        else if (value == CDMI_ACE_WRITE_OWNER)
            result = STR_CDMI_ACE_WRITE_OWNER;
        else if (value == CDMI_ACE_SYNCHRONIZE)
            result = STR_CDMI_ACE_SYNCHRONIZE;
        return result;
    }

    public int toValue(String string)
    {
        int result = 0;
        //switch can't be used here because some values repeat
        switch (string) {
            case STR_CDMI_ACE_ACCESS_ALLOW:
                result = CDMI_ACE_ACCESS_ALLOW;
                break;
            case STR_CDMI_ACE_ACCESS_DENY:
                result = CDMI_ACE_ACCESS_DENY;
                break;
            case STR_CDMI_ACE_SYSTEM_AUDIT:
                result = CDMI_ACE_SYSTEM_AUDIT;
                break;
            case STR_CDMI_ACE_FLAGS_NONE:
                result = CDMI_ACE_FLAGS_NONE;
                break;
            case STR_CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE:
                result = CDMI_ACE_FLAGS_OBJECT_INHERIT_ACE;
                break;
            case STR_CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE:
                result = CDMI_ACE_FLAGS_CONTAINER_INHERIT_ACE;
                break;
            case STR_CDMI_ACE_FLAGS_NO_PROPAGATE_ACE:
                result = CDMI_ACE_FLAGS_NO_PROPAGATE_ACE;
                break;
            case STR_CDMI_ACE_FLAGS_INHERIT_ONLY_ACE:
                result = CDMI_ACE_FLAGS_INHERIT_ONLY_ACE;
                break;
            case STR_CDMI_ACE_FLAGS_IDENTIFIER_GROUP:
                result = CDMI_ACE_FLAGS_IDENTIFIER_GROUP;
                break;
            case STR_CDMI_ACE_FLAGS_INHERITED_ACE:
                result = CDMI_ACE_FLAGS_INHERITED_ACE;
                break;
            case STR_CDMI_ACE_READ_OBJECT:
                result = CDMI_ACE_READ_OBJECT;
                break;
            case STR_CDMI_ACE_LIST_CONTAINER:
                result = CDMI_ACE_LIST_CONTAINER;
                break;
            case STR_CDMI_ACE_WRITE_OBJECT:
                result = CDMI_ACE_WRITE_OBJECT;
                break;
            case STR_CDMI_ACE_ADD_OBJECT:
                result = CDMI_ACE_ADD_OBJECT;
                break;
            case STR_CDMI_ACE_APPEND_DATA:
                result = CDMI_ACE_APPEND_DATA;
                break;
            case STR_CDMI_ACE_ADD_SUBCONTAINER:
                result = CDMI_ACE_ADD_SUBCONTAINER;
                break;
            case STR_CDMI_ACE_READ_METADATA:
                result = CDMI_ACE_READ_METADATA;
                break;
            case STR_CDMI_ACE_WRITE_METADATA:
                result = CDMI_ACE_WRITE_METADATA;
                break;
            case STR_CDMI_ACE_EXECUTE:
                result = CDMI_ACE_EXECUTE;
                break;
            case STR_CDMI_ACE_DELETE_OBJECT:
                result = CDMI_ACE_DELETE_OBJECT;
                break;
            case STR_CDMI_ACE_DELETE_SUBCONTAINER:
                result = CDMI_ACE_DELETE_SUBCONTAINER;
                break;
            case STR_CDMI_ACE_READ_ATTRIBUTES:
                result = CDMI_ACE_READ_ATTRIBUTES;
                break;
            case STR_CDMI_ACE_WRITE_ATTRIBUTES:
                result = CDMI_ACE_WRITE_ATTRIBUTES;
                break;
            case STR_CDMI_ACE_WRITE_RETENTION:
                result = CDMI_ACE_WRITE_RETENTION;
                break;
            case STR_CDMI_ACE_WRITE_RETENTION_HOLD:
                result = CDMI_ACE_WRITE_RETENTION_HOLD;
                break;
            case STR_CDMI_ACE_DELETE:
                result = CDMI_ACE_DELETE;
                break;
            case STR_CDMI_ACE_READ_ACL:
                result = CDMI_ACE_READ_ACL;
                break;
            case STR_CDMI_ACE_WRITE_ACL:
                result = CDMI_ACE_WRITE_ACL;
                break;
            case STR_CDMI_ACE_WRITE_OWNER:
                result = CDMI_ACE_WRITE_OWNER;
                break;
            case STR_CDMI_ACE_SYNCHRONIZE:
                result = CDMI_ACE_SYNCHRONIZE;
                break;
        }
        return result;
    }
}
