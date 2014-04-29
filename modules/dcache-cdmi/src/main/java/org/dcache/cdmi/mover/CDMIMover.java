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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
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

        _log.trace("\n\trunIO()\n\tprotocol="+protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+",\n\taccess="+access);

        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        if (access == IoMode.WRITE) {
            writeFile();
        } else if (access == IoMode.READ) {
            readFileAsBytes();
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

    private void writeFile() throws ACLException
    {
        if (channel.isOpen()) {
            try {
                strData = DcacheDataTransfer.getDataAsString();
                ByteBuffer data = stringToByteBuffer(strData);
                channel.write(data);
                _log.trace("CDMIMover data written:|" + strData + "|[" + strData.length() + " bytes]");
                DcacheDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                DcacheDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                DcacheDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                DcacheDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                DcacheDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                DcacheDataTransfer.setSize(channel.getFileAttributes().getSize());
                DcacheDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                DcacheDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                DcacheDataTransfer.setACL(channel.getFileAttributes().getAcl());
            } catch (IOException ex) {
                _log.error("Data could not be written into CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsBytes() throws ACLException
    {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    DcacheDataTransfer.setData(data.array());
                    strData = byteBufferToString(data);
                    _log.trace("CDMIMover data read:|" + strData + "|[" + dataSize + " bytes]");
                    DcacheDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                    DcacheDataTransfer.setSize(channel.getFileAttributes().getSize());
                    DcacheDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                    DcacheDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                    DcacheDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                    DcacheDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                    DcacheDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                    DcacheDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                    DcacheDataTransfer.setACL(channel.getFileAttributes().getAcl());
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsString() throws ACLException
    {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    strData = byteBufferToString(data);
                    _log.trace("CDMIMover data read:|" + strData + "|[" + dataSize + " bytes]");
                    DcacheDataTransfer.setData(strData);
                    DcacheDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                    DcacheDataTransfer.setSize(channel.getFileAttributes().getSize());
                    DcacheDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                    DcacheDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                    DcacheDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                    DcacheDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                    DcacheDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                    DcacheDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                    DcacheDataTransfer.setACL(channel.getFileAttributes().getAcl());
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    public void closeChannel()
    {
        if (channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException ex) {
                _log.error("CDMI channel could not be closed, exception is: " + ex.getMessage());
            }
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