/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.cdmi.mover;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import org.dcache.acl.ACLException;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverProtocol;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CDMIMover implements MoverProtocol
{
    private static final Logger _log = LoggerFactory.getLogger(CDMIMover.class);
    private final CellEndpoint cell;
    private CDMIProtocolInfo pi;
    private MoverChannel<CDMIProtocolInfo> channel;
    private ServerSocketChannel serverSocketChannel = null;
    private SocketChannel socketChannel = null;
    private ObjectOutputStream  oos = null;
    private ObjectInputStream ois = null;
    private static final int port = 9999;
    private String strData = "";

    public CDMIMover(CellEndpoint endpoint)
    {
        this.cell = endpoint;
    }

    @Override
    public void runIO(FileAttributes fileAttributes, RepositoryChannel diskFile,
                      ProtocolInfo protocol, Allocator allocator, IoMode access) throws CacheException, ACLException
    {
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storage = fileAttributes.getStorageInfo();
        pi = (CDMIProtocolInfo) protocol;

        _log.trace("\n\trunIO()\n\tprotocol={},\n\tStorageInfo={},\n\tPnfsId={},\n\taccess={}"+access, protocol, storage, pnfsId, access);

        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(port));
        } catch (BindException ex) {
            //Do nothing here, might occur if Client tries to reconnect
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        if (access == IoMode.WRITE) {
            writeFile();
        } else if (access == IoMode.READ) {
            readFile();
        }

        try {
            if (socketChannel != null) socketChannel.close();
            if (serverSocketChannel != null) serverSocketChannel.close();
        } catch (IOException ex) {
            ex.printStackTrace();
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

    private synchronized void writeFile()
    {
        try {
            socketChannel = serverSocketChannel.accept();
            if ((socketChannel != null) && socketChannel.isOpen() && socketChannel.isConnected()) {
                ois = new ObjectInputStream(socketChannel.socket().getInputStream());
                String dataRead1 = (String) ois.readObject();
                strData = dataRead1;
                ois.close();
            }
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        if (channel.isOpen()) {
            try {
                ByteBuffer data = stringToByteBuffer(strData);
                channel.write(data);
                _log.trace("CDMIMover data written:[{}|[{} bytes]", strData, strData.length());
            } catch (IOException ex) {
                _log.error("Data could not be written into CDMI channel, {}", ex.getMessage());
            }
        }
    }

    private synchronized void readFile()
    {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    strData = byteBufferToString(data);
                    _log.trace("CDMIMover data read:[{}|[{} bytes]", strData, strData.length());
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, {}", ex.getMessage());
            }
        }
        try {
            socketChannel = serverSocketChannel.accept();
            if ((socketChannel != null) && socketChannel.isOpen() && socketChannel.isConnected()) {
                oos = new ObjectOutputStream(socketChannel.socket().getOutputStream());
                String dataWrite1 = strData;
                oos.writeObject(dataWrite1);
                oos.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private synchronized static ByteBuffer stringToByteBuffer(String data)
    {
        return ByteBuffer.wrap(data.getBytes());
    }

    private synchronized static String byteBufferToString(ByteBuffer data)
    {
        String result = "";
        try {
            result = new String(data.array(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
        }
        return result;
    }

}