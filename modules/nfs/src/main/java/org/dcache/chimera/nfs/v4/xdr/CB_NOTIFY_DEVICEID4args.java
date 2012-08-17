/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class CB_NOTIFY_DEVICEID4args implements XdrAble {
    public notify4 [] cnda_changes;

    public CB_NOTIFY_DEVICEID4args() {
    }

    public CB_NOTIFY_DEVICEID4args(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        { int $size = cnda_changes.length; xdr.xdrEncodeInt($size); for ( int $idx = 0; $idx < $size; ++$idx ) { cnda_changes[$idx].xdrEncode(xdr); } }
    }

    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        { int $size = xdr.xdrDecodeInt(); cnda_changes = new notify4[$size]; for ( int $idx = 0; $idx < $size; ++$idx ) { cnda_changes[$idx] = new notify4(xdr); } }
    }

}
// End of CB_NOTIFY_DEVICEID4args.java