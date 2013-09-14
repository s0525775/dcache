/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.dcache.cdmi.util;

import javax.security.auth.Subject;

import java.net.InetSocketAddress;
import java.util.UUID;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellPath;

import org.dcache.util.RedirectedTransfer;

public class CDMITransfer extends RedirectedTransfer<InetSocketAddress>
{
    private UUID _uuid;
    private InetSocketAddress _doorAddress;
    private int _fileHandle;

    public final static String CDMI_PROTOCOL_STRING = "CDMI";
    public final static int CDMI_PROTOCOL_MAJOR_VERSION = 2;
    public final static int CDMI_PROTOCOL_MINOR_VERSION = 7;
    public final static String CDMI_PROTOCOL_VERSION =
        String.format("%d.%d",
                      CDMI_PROTOCOL_MAJOR_VERSION,
                      CDMI_PROTOCOL_MINOR_VERSION);

    public CDMITransfer(PnfsHandler pnfs, Subject subject, FsPath path) {
        super(pnfs, subject, path);
    }

    public synchronized void setFileHandle(int fileHandle) {
        _fileHandle = fileHandle;
    }

    public synchronized int getFileHandle() {
        return _fileHandle;
    }

    public synchronized void setUUID(UUID uuid) {
        _uuid = uuid;
    }

    public synchronized void setDoorAddress(InetSocketAddress doorAddress) {
        _doorAddress = doorAddress;
    }

    protected synchronized ProtocolInfo createProtocolInfo() {
        InetSocketAddress client = getClientAddress();
        CDMIProtocolinfo protocolInfo =
            new CDMIProtocolinfo(CDMI_PROTOCOL_STRING,
                                 CDMI_PROTOCOL_MAJOR_VERSION,
                                 CDMI_PROTOCOL_MINOR_VERSION,
                                 client,
                                 new CellPath(getCellName(), getDomainName()),
                                 getPnfsId(),
                                 _fileHandle,
                                 _uuid,
                                 _doorAddress);
        protocolInfo.setPath(_path.toString());
        return protocolInfo;
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPoolManager() {
        return createProtocolInfo();
    }

    @Override
    protected ProtocolInfo getProtocolInfoForPool() {
        return createProtocolInfo();
    }
}

