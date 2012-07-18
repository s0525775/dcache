/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class CREATE4args implements XdrAble {
    public createtype4 objtype;
    public component4 objname;
    public fattr4 createattrs;

    public CREATE4args() {
    }

    public CREATE4args(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        objtype.xdrEncode(xdr);
        objname.xdrEncode(xdr);
        createattrs.xdrEncode(xdr);
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        objtype = new createtype4(xdr);
        objname = new component4(xdr);
        createattrs = new fattr4(xdr);
    }

}
// End of CREATE4args.java