/*
 * Automatically generated by jrpcgen 1.0.7 on 2/21/09 1:22 AM
 * jrpcgen is part of the "Remote Tea" ONC/RPC package for Java
 * See http://remotetea.sourceforge.net for details
 */
package org.dcache.chimera.nfs.v4.xdr;
import org.dcache.chimera.nfs.v4.*;
import org.dcache.xdr.*;
import java.io.IOException;

public class EXCHANGE_ID4args implements XdrAble {
    public client_owner4 eia_clientowner;
    public uint32_t eia_flags;
    public state_protect4_a eia_state_protect;
    public nfs_impl_id4 [] eia_client_impl_id;

    public EXCHANGE_ID4args() {
    }

    public EXCHANGE_ID4args(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        xdrDecode(xdr);
    }

    @Override
    public void xdrEncode(XdrEncodingStream xdr)
           throws OncRpcException, IOException {
        eia_clientowner.xdrEncode(xdr);
        eia_flags.xdrEncode(xdr);
        eia_state_protect.xdrEncode(xdr);
        { int $size = eia_client_impl_id.length; xdr.xdrEncodeInt($size); for ( int $idx = 0; $idx < $size; ++$idx ) { eia_client_impl_id[$idx].xdrEncode(xdr); } }
    }

    @Override
    public void xdrDecode(XdrDecodingStream xdr)
           throws OncRpcException, IOException {
        eia_clientowner = new client_owner4(xdr);
        eia_flags = new uint32_t(xdr);
        eia_state_protect = new state_protect4_a(xdr);
        { int $size = xdr.xdrDecodeInt(); eia_client_impl_id = new nfs_impl_id4[$size]; for ( int $idx = 0; $idx < $size; ++$idx ) { eia_client_impl_id[$idx] = new nfs_impl_id4(xdr); } }
    }

}
// End of EXCHANGE_ID4args.java