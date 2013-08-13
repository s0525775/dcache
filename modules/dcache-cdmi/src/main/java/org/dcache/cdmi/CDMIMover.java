package org.dcache.cdmi;

import com.google.common.io.ByteStreams;

import java.nio.channels.SocketChannel;

import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;

public class CDMIMover implements MoverProtocol
{
    private MoverChannel<CDMIProtocolInfo> channel;

    public CDMIMover(CellEndpoint endpoint)
    {
    }

    @Override
    public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile,
                      ProtocolInfo protocol, Allocator allocator, IoMode access) throws Exception
    {
        CDMIProtocolInfo pi = (CDMIProtocolInfo) protocol;
        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        try (SocketChannel connection = SocketChannel.open(pi.getSocketAddress())) {
            ByteStreams.copy(connection, channel);
        }
    }

    @Override
    public long getBytesTransferred()
    {
        return channel.getBytesTransferred();
    }

    @Override
    public long getTransferTime()
    {
        return channel.getTransferTime();
    }

    @Override
    public long getLastTransferred()
    {
        return channel.getLastTransferred();
    }
}