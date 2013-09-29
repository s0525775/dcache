package org.dcache.cdmi.mover;

import diskCacheV111.util.PnfsHandler;
import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;

public class CDMIDataTransfer
{

    //BACKUP
    private static CellStub pnfsStub;
    private static PnfsHandler pnfsHandler;
    private static ListDirectoryHandler listDirectoryHandler;
    private static CellStub poolStub;
    private static CellStub poolMgrStub;
    private static CellStub billingStub;
    private static CellStub pnfsStub2;
    private static PnfsHandler pnfsHandler2;
    private static ListDirectoryHandler listDirectoryHandler2;
    private static CellStub poolStub2;
    private static CellStub poolMgrStub2;
    private static CellStub billingStub2;
    private static byte[] bytData;
    private static String strData;

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

}