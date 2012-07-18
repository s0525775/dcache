/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class BIND_CONN_TO_SESSION4res implements XdrAble {
    public int bctsr_status;
    public BIND_CONN_TO_SESSION4resok bctsr_resok4;

    public BIND_CONN_TO_SESSION4res() {
    }

    public BIND_CONN_TO_SESSION4res(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        xdr.xdrEncodeInt(bctsr_status);
        switch ( bctsr_status ) {
        case nfsstat.NFS_OK:
            bctsr_resok4.xdrEncode(xdr);
            break;
        default:
            break;
        }
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        bctsr_status = xdr.xdrDecodeInt();
        switch ( bctsr_status ) {
        case nfsstat.NFS_OK:
            bctsr_resok4 = new BIND_CONN_TO_SESSION4resok(xdr);
            break;
        default:
            break;
        }
    }

}
// End of BIND_CONN_TO_SESSION4res.java