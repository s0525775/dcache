package org.dcache.cdmi.mover;

public class CDMIDataTransfer
{

    private static String _data;

    public static void setData(String data)
    {
        _data = data;
    }

    public static String getData()
    {
        return _data;
    }

}