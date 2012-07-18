/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.xdr.*;
import java.io.IOException;

public class fattr4_retentevt_set implements XdrAble {

    public retention_set4 value;

    public fattr4_retentevt_set() {
    }

    public fattr4_retentevt_set(retention_set4 value) {
        this.value = value;
    }

    public fattr4_retentevt_set(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        value.xdrEncode(xdr);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        value = new retention_set4(xdr);
    }

}
// End of fattr4_retentevt_set.java