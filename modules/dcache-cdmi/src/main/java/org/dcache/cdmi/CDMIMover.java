package org.dcache.cdmi;

import com.google.common.io.ByteStreams;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.DCapClientPortAvailableMessage;

import java.nio.channels.SocketChannel;

import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import org.dcache.cdmi.temp.Test;

import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.util.NetworkUtils;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDMIMover implements MoverProtocol
{
    private static final Logger _log =
        LoggerFactory.getLogger(CDMIMover.class);
    private long last_transfer_time = System.currentTimeMillis();
    private final CellEndpoint cell;
    private CDMIProtocolInfo pi;
    private long starttime;
    private volatile long transfered;
    private MoverChannel<CDMIProtocolInfo> channel;

    public CDMIMover(CellEndpoint endpoint)
    {
        this.cell = endpoint;
        Test.write("/tmp/testb001.log", "c011");
    }

    //No write pool available for <size=0;new=true;stored=false;sClass=test:disk;cClass=-;hsm=osm;
    //accessLatency=NEARLINE;retentionPolicy=CUSTODIAL;uid=-1;gid=-1;path=/disk/test.txt;StoreName=
    //test;store=test;group=disk;bfid=<Unknown>;> in the linkGroup [none]

    @Override
    public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile,
                      ProtocolInfo protocol, Allocator allocator, IoMode access) throws Exception
    {

        /*
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storage = fileAttributes.getStorageInfo();
        _log.warn("runIO()\n\tprotocol="+
            protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+
            ",\n\taccess ="+access);
        if (!(protocol instanceof CDMIProtocolInfo)) {
                throw new  CacheException("protocol info is not CDMIProtocolInfo");
        }
        starttime = System.currentTimeMillis();

        Test.write("/tmp/testb001.log", "c012");
        pi = (CDMIProtocolInfo) protocol;

        CellPath cellpath = new CellPath(pi.getInitiatorCellName(),
                                         pi.getInitiatorCellDomain());
        _log.warn("runIO() CDMITranferManager cellpath="+cellpath);
        */

        //old:
        Test.write("/tmp/testb001.log", "c013");
        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        try (SocketChannel connection = SocketChannel.open(pi.getSocketAddress())) {
            Test.write("/tmp/testb001.log", "c014");
            ByteStreams.copy(connection, channel);
            Test.write("/tmp/testb001.log", "c015" + String.valueOf(channel.getBytesTransferred()));
        }

        diskFile.write(Test.stringToByteBuffer("Hello"), 50);
        Test.write("/tmp/testb001.log", "c015" + String.valueOf(channel.getBytesTransferred()));

        /*
        ServerSocket serverSocket;
        try{
            serverSocket = new ServerSocket(0, 1);
        } catch(IOException ioe) {
            _log.warn("exception while trying to create a server socket : " + ioe);
            throw ioe;
        }
        int port = serverSocket.getLocalPort();

        InetAddress localAddress = NetworkUtils.getLocalAddress(pi.getSocketAddress().getAddress());

        Socket dcap_socket = serverSocket.accept();
        _log.warn("connected");
        try {
            serverSocket.close();
        } catch(IOException ioe) {
            _log.warn("failed to close server socket");
            _log.warn(ioe.getMessage());
            // we still can continue, this is non-fatal
        }

        if (access == IoMode.WRITE) {
            cdmiWriteFile(dcap_socket, diskFile, allocator);
        } else {
            //cdmiReadFile(dcap_socket, diskFile, allocator);
            throw new IOException("read is not implemented");
        }
        _log.warn("runIO() done");
        */

        /*
        if (channel.isOpen()) {
            Test.write("/tmp/testb001.log", "c016");
            channel.write(Test.stringToByteBuffer("HELLO"));
            Test.write("/tmp/testb001.log", "c017" + String.valueOf(channel.getBytesTransferred()));
        }
        if (channel.isOpen()) {
            ByteBuffer dst = ByteBuffer.allocate(50);
            Test.write("/tmp/testb001.log", "c018");
            channel.read(dst);
            Test.write("/tmp/testb001.log", "c019" + String.valueOf(channel.getBytesTransferred()));
            Test.write("/tmp/testc001.log", Test.byteBufferToString(dst));
        }
        if (channel.isOpen()) {
            Test.write("/tmp/testb001.log", "c020");
            channel.close();
            Test.write("/tmp/testb001.log", "c021");
        }
        */
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

    private void cdmiWriteFile(Socket socket,
                              RepositoryChannel diskFile,
                              Allocator allocator) throws Exception
    {
        last_transfer_time    = System.currentTimeMillis();
        DataInputStream in   = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        //diskFile.write(null);
        _log.warn("<Done>");
    }
}