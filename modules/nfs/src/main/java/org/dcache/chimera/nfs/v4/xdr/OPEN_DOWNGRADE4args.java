/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class OPEN_DOWNGRADE4args implements XdrAble {
    public stateid4 open_stateid;
    public seqid4 seqid;
    public uint32_t share_access;
    public uint32_t share_deny;

    public OPEN_DOWNGRADE4args() {
    }

    public OPEN_DOWNGRADE4args(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        open_stateid.xdrEncode(xdr);
        seqid.xdrEncode(xdr);
        share_access.xdrEncode(xdr);
        share_deny.xdrEncode(xdr);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        open_stateid = new stateid4(xdr);
        seqid = new seqid4(xdr);
        share_access = new uint32_t(xdr);
        share_deny = new uint32_t(xdr);
    }

}
// End of OPEN_DOWNGRADE4args.java