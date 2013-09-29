package org.dcache.cdmi.mover;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellEndpoint;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import org.dcache.cdmi.temp.Test;
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
                      ProtocolInfo protocol, Allocator allocator, IoMode access) throws CacheException
    {
        PnfsId pnfsId = fileAttributes.getPnfsId();
        StorageInfo storage = fileAttributes.getStorageInfo();

        _log.error("\n\trunIO()\n\tprotocol="+protocol+",\n\tStorageInfo="+storage+",\n\tPnfsId="+pnfsId+",\n\taccess="+access);

        channel = new MoverChannel<>(access, fileAttributes, pi, diskFile, allocator);
        if (access == IoMode.WRITE) {
            writeFile();
        } else if (access == IoMode.READ) {
            readFileAsBytes();
        }
        closeChannel();
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

    private void writeFile() {
        if (channel.isOpen()) {
            try {
                strData = CDMIDataTransfer.getDataAsString();
                ByteBuffer data = Test.stringToByteBuffer(strData);
                channel.write(data);
                channel.sync();
            } catch (IOException ex) {
                _log.error("Data could not be written into CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsBytes() {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    CDMIDataTransfer.setData(data.array());
                    _log.error("CDMIMover data read:|" + data.array().toString() + "|[" + dataSize + " bytes]");
                }
            } catch (IOException ex) {
                _log.error("Data could not be read from CDMI channel, exception is: " + ex.getMessage());
            }
        }
    }

    private void readFileAsString() {
        if (channel.isOpen()) {
            try {
                int dataSize = (int) channel.getFileAttributes().getSize();
                if (dataSize > 0) {
                    channel.position(0);
                    ByteBuffer data = ByteBuffer.allocate(dataSize);
                    channel.read(data);
                    strData = Test.byteBufferToString(data);
                    CDMIDataTransfer.setData(strData);
                    _log.error("CDMIMover data read:|" + strData + "|[" + dataSize + " bytes]");
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

}