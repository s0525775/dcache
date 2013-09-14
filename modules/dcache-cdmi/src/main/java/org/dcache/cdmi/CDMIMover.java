package org.dcache.cdmi;

import com.google.common.io.ByteStreams;

import java.nio.channels.SocketChannel;

import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEndpoint;
import java.nio.ByteBuffer;
import org.dcache.cdmi.temp.Test;

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
        Test.write("/tmp/testb001.log", "011");
    }

    //No write pool available for <size=0;new=true;stored=false;sClass=test:disk;cClass=-;hsm=osm;
    //accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;uid=-1;gid=-1;path=/disk/test.txt;StoreName=
    //test;store=test;group=disk;bfid=<Unknown>;> in the linkGroup [none]

    @Override
    public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile,
                      ProtocolInfo protocol, Allocator allocator, IoMode access) throws Exception
    {
        Test.write("/tmp/testb001.log", "012");
        CDMIProtocolInfo pi = (CDMIProtocolInfo) protocol;
        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        try (SocketChannel connection = SocketChannel.open(pi.getSocketAddress())) {
            ByteStreams.copy(connection, channel);
        }
        channel.write(Test.stringToByteBuffer("HELLO"));
        ByteBuffer dst = ByteBuffer.allocate(50);
        channel.read(dst);
        Test.write("/tmp/testc001.log", Test.byteBufferToString(dst));
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