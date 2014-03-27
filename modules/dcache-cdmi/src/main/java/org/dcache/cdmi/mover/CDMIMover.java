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

        _log.debug("\n\trunIO()\n\tprotocol="+protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+",\n\taccess="+access);

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

    private void writeFile() throws ACLException {
        if (channel.isOpen()) {
            try {
                strData = CDMIDataTransfer.getDataAsString();
                ByteBuffer data = stringToByteBuffer(strData);
                channel.write(data);
                _log.debug("CDMIMover data written:|" + strData + "|[" + strData.length() + " bytes]");
                CDMIDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                CDMIDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                CDMIDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                CDMIDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                CDMIDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                CDMIDataTransfer.setSize(channel.getFileAttributes().getSize());
                CDMIDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                CDMIDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                CDMIDataTransfer.setACL(channel.getFileAttributes().getAcl());
            } catch (IOException ex) {
                _log.error("Data could not be written into CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsBytes() throws ACLException {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    CDMIDataTransfer.setData(data.array());
                    strData = byteBufferToString(data);
                    _log.debug("CDMIMover data read:|" + strData + "|[" + dataSize + " bytes]");
                    CDMIDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                    CDMIDataTransfer.setSize(channel.getFileAttributes().getSize());
                    CDMIDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                    CDMIDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                    CDMIDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                    CDMIDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                    CDMIDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                    CDMIDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                    CDMIDataTransfer.setACL(channel.getFileAttributes().getAcl());
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsString() throws ACLException {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    strData = byteBufferToString(data);
                    _log.debug("CDMIMover data read:|" + strData + "|[" + dataSize + " bytes]");
                    CDMIDataTransfer.setData(strData);
                    CDMIDataTransfer.setPnfsId(channel.getFileAttributes().getPnfsId());
                    CDMIDataTransfer.setSize(channel.getFileAttributes().getSize());
                    CDMIDataTransfer.setOwner(channel.getFileAttributes().getOwner());
                    CDMIDataTransfer.setAccessTime(channel.getFileAttributes().getAccessTime());
                    CDMIDataTransfer.setChangeTime(channel.getFileAttributes().getChangeTime());
                    CDMIDataTransfer.setModificationTime(channel.getFileAttributes().getModificationTime());
                    CDMIDataTransfer.setCreationTime(channel.getFileAttributes().getCreationTime());
                    CDMIDataTransfer.setFileType(channel.getFileAttributes().getFileType());
                    CDMIDataTransfer.setACL(channel.getFileAttributes().getAcl());
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    public void closeChannel() {
        if (channel.isOpen()) {
            try {
                channel.close();
            } catch (IOException ex) {
                _log.error("CDMI channel could not be closed, exception is: " + ex.getMessage());
            }
        }
    }

    private synchronized static ByteBuffer stringToByteBuffer(String data) {
        return ByteBuffer.wrap(data.getBytes());
    }

    private synchronized static String byteBufferToString(ByteBuffer data) {
        String result = "";
        try {
            result = new String(data.array(), "UTF-8");
        } catch (UnsupportedEncodingException ex) {
        }
        return result;
    }

}