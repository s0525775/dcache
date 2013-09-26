package org.dcache.cdmi.mover;

import java.net.InetSocketAddress;

import diskCacheV111.vehicles.IpProtocolInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDMIProtocolInfo implements IpProtocolInfo
{
    private static final Logger _log = LoggerFactory.getLogger(CDMIMover.class);
    private final InetSocketAddress address;

    public CDMIProtocolInfo(InetSocketAddress address)
    {
        this.address = address;
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