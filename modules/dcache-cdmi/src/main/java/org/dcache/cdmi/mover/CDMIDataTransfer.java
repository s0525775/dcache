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

    public static String getDataAsString()
    {
        return strData;
    }

    public static byte[] getDataAsBytes()
    {
        return bytData;
    }

}