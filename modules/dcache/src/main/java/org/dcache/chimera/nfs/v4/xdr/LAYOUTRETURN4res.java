/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class LAYOUTRETURN4res implements XdrAble {
    public int lorr_status;
    public layoutreturn_stateid lorr_stateid;

    public LAYOUTRETURN4res() {
    }

    public LAYOUTRETURN4res(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        xdr.xdrEncodeInt(lorr_status);
        switch ( lorr_status ) {
        case nfsstat4.NFS4_OK:
            lorr_stateid.xdrEncode(xdr);
            break;
        default:
            break;
        }
    }

    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        lorr_status = xdr.xdrDecodeInt();
        switch ( lorr_status ) {
        case nfsstat4.NFS4_OK:
            lorr_stateid = new layoutreturn_stateid(xdr);
            break;
        default:
            break;
        }
    }

}
// End of LAYOUTRETURN4res.java