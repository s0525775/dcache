/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.xdr.*;
import java.io.IOException;

public class READDIR4resok implements XdrAble {
    public verifier4 cookieverf;
    public dirlist4 reply;

    public READDIR4resok() {
    }

    public READDIR4resok(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        cookieverf.xdrEncode(xdr);
        reply.xdrEncode(xdr);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        cookieverf = new verifier4(xdr);
        reply = new dirlist4(xdr);
    }

}
// End of READDIR4resok.java