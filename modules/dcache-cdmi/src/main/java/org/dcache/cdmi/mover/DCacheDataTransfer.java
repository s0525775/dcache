package org.dcache.cdmi.mover;

import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import org.dcache.acl.ACL;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileType;
import org.dcache.util.list.ListDirectoryHandler;

// This class is not nice, I'm still searching for a better solution here sometime.
// But it works for the moment. This class is a workaround which shall solve problems
// which are caused by another workaround which was suggested by Apache/Eclipse.
// The workaround of Apache/Eclipse suggested that a class, which has a servlet
// listener implemented, gets initialized twice. That class receives additional
// arguments during the first initialization. Since the functions of that class will
// be executed after a second initialization with the new command, those arguments
// (which are stored in that class) will be lost then - unless this static class is
// used.

public class DCacheDataTransfer
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