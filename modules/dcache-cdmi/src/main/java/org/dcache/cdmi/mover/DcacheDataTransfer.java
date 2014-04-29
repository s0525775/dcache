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
package org.dcache.cdmi.mover;

import diskCacheV111.util.PnfsId;
import org.dcache.acl.ACL;
import org.dcache.namespace.FileType;

// This class is not nice (quick-and-dirty), I'm still searching for a better solution here.
// It just keeps some data in a static state at the moment so that the data doesn't get lost.
// The class will be replaced very soon.

public class DcacheDataTransfer
{

    //BACKUP
    private static byte[] bytData;
    private static String strData;
    private static PnfsId pnfsId;
    private static long accessTime;
    private static long creationTime;
    private static long changeTime;
    private static long modificationTime;
    private static long size;
    private static int owner;
    private static ACL acl;
    private static FileType fileType;

    public static void setData(String data)
    {
        strData = data;
    }

    public static void setData(byte[] data)
    {
        bytData = data;
    }

    public static void setPnfsId(PnfsId value)
    {
        pnfsId = value;
    }

    public static void setAccessTime(long value)
    {
        accessTime = value;
    }

    public static void setCreationTime(long value)
    {
        creationTime = value;
    }

    public static void setChangeTime(long value)
    {
        changeTime = value;
    }

    public static void setModificationTime(long value)
    {
        modificationTime = value;
    }

    public static void setSize(long value)
    {
        size = value;
    }

    public static void setOwner(int value)
    {
        owner = value;
    }

    public static void setACL(ACL value)
    {
        acl = value;
    }

    public static void setFileType(FileType value)
    {
        fileType = value;
    }

    public static String getDataAsString()
    {
        return strData;
    }

    public static byte[] getDataAsBytes()
    {
        return bytData;
    }

    public static PnfsId getPnfsId()
    {
        return pnfsId;
    }

    public static long getAccessTime()
    {
        return accessTime;
    }

    public static long getCreationTime()
    {
        return creationTime;
    }

    public static long getChangeTime()
    {
        return changeTime;
    }

    public static long getModificationTime()
    {
        return modificationTime;
    }

    public static long getSize()
    {
        return size;
    }

    public static ACL getACL()
    {
        return acl;
    }

    public static int getOwner()
    {
        return owner;
    }

    public static FileType getFileType()
    {
        return fileType;
    }

}