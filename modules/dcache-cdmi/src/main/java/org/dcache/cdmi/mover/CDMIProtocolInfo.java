package org.dcache.cdmi.mover;

import java.net.InetSocketAddress;

import diskCacheV111.vehicles.IpProtocolInfo;
import org.dcache.cdmi.temp.Test;

public class CDMIProtocolInfo implements IpProtocolInfo
{
    private final InetSocketAddress address;
    private final String cellDomainName;
    private final String cellName;

    public CDMIProtocolInfo(InetSocketAddress address)
    {
        Test.write("/tmp/testb001.log", "010");
        this.address = address;
        this.cellDomainName = "";
        this.cellName = "";
    }

    public CDMIProtocolInfo(String cellDomainName, String domainName, InetSocketAddress address)
    {
        Test.write("/tmp/testb001.log", "010");
        this.address = address;
        this.cellDomainName = cellDomainName;
        this.cellName = domainName;
    }

    public String getInitiatorCellDomain()
    {
        return cellDomainName;
    }

    public String getInitiatorCellName()
    {
        return cellName;
    }

    @Override
    public String getProtocol()
    {
        return "CDMI";
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    @Override
    public String getVersionString()
    {
        return "CDMI-1.0";
    }

    @Override
    public InetSocketAddress getSocketAddress()
    {
        return address;
    }
}