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

public class CDMIDataTransfer
{

    //BACKUP
    private static CellStub pnfsStub;
    private static PnfsHandler pnfsHandler;
    private static ListDirectoryHandler listDirectoryHandler;
    private static CellStub poolStub;
    private static CellStub poolMgrStub;
    private static CellStub billingStub;
    private static String baseDirectoryName;
    private static CellStub pnfsStub2;
    private static PnfsHandler pnfsHandler2;
    private static ListDirectoryHandler listDirectoryHandler2;
    private static CellStub poolStub2;
    private static CellStub poolMgrStub2;
    private static CellStub billingStub2;
    private static String baseDirectoryName2;
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

    public static void setBaseDirectoryName(String directoryName)
    {
        baseDirectoryName = directoryName;
    }

    public static void setPnfsStub(CellStub stub)
    {
        pnfsStub = stub;
    }

    public static void setPnfsHandler(PnfsHandler handler)
    {
        pnfsHandler = handler;
    }

    public static void setListDirectoryHandler(ListDirectoryHandler handler)
    {
        listDirectoryHandler = handler;
    }

    public static void setPoolStub(CellStub stub)
    {
        poolStub = stub;
    }

    public static void setPoolMgrStub(CellStub stub)
    {
        poolMgrStub = stub;
    }

    public static void setBillingStub(CellStub stub)
    {
        billingStub = stub;
    }

    public static void setBaseDirectoryName2(String directoryName)
    {
        baseDirectoryName2 = directoryName;
    }

    public static void setPnfsStub2(CellStub stub)
    {
        pnfsStub2 = stub;
    }

    public static void setPnfsHandler2(PnfsHandler handler)
    {
        pnfsHandler2 = handler;
    }

    public static void setListDirectoryHandler2(ListDirectoryHandler handler)
    {
        listDirectoryHandler2 = handler;
    }

    public static void setPoolStub2(CellStub stub)
    {
        poolStub2 = stub;
    }

    public static void setPoolMgrStub2(CellStub stub)
    {
        poolMgrStub2 = stub;
    }

    public static void setBillingStub2(CellStub stub)
    {
        billingStub2 = stub;
    }

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

    public static String getBaseDirectoryName()
    {
        return baseDirectoryName;
    }

    public static CellStub getPnfsStub()
    {
        return pnfsStub;
    }

    public static PnfsHandler getPnfsHandler()
    {
        return pnfsHandler;
    }

    public static ListDirectoryHandler getListDirectoryHandler()
    {
        return listDirectoryHandler;
    }

    public static CellStub getPoolStub()
    {
        return poolStub;
    }

    public static CellStub getPoolMgrStub()
    {
        return poolMgrStub;
    }

    public static CellStub getBillingStub()
    {
        return billingStub;
    }

    public static String getBaseDirectoryName2()
    {
        return baseDirectoryName2;
    }

    public static CellStub getPnfsStub2()
    {
        return pnfsStub2;
    }

    public static PnfsHandler getPnfsHandler2()
    {
        return pnfsHandler2;
    }

    public static ListDirectoryHandler getListDirectoryHandler2()
    {
        return listDirectoryHandler2;
    }

    public static CellStub getPoolStub2()
    {
        return poolStub2;
    }

    public static CellStub getPoolMgrStub2()
    {
        return poolMgrStub2;
    }

    public static CellStub getBillingStub2()
    {
        return billingStub2;
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