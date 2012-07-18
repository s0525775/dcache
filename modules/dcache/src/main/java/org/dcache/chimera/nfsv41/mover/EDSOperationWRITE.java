package org.dcache.chimera.nfsv41.mover;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.chimera.nfs.v4.AbstractNFSv4Operation;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.CompoundContext;
import org.dcache.chimera.nfs.v4.xdr.*;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.RepositoryChannel;


public class EDSOperationWRITE extends AbstractNFSv4Operation {

    private static final Logger _log = LoggerFactory.getLogger(EDSOperationWRITE.class.getName());

     private final Map<stateid4, MoverBridge> _activeIO;
     private static final int INC_SPACE = (50 * 1024 * 1024);


    public EDSOperationWRITE(nfs_argop4 args, Map<stateid4, MoverBridge> activeIO) {
        super(args, nfs_opnum4.OP_WRITE);
        _activeIO = activeIO;
    }

    @Override
    public nfs_resop4 process(CompoundContext context) {

        WRITE4res res = new WRITE4res();

        try {

            MoverBridge moverBridge = _activeIO.get( _args.opwrite.stateid);
            if (moverBridge == null) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BAD_STATEID,
                        "No mover associated with given stateid");
            }

            if( moverBridge.getIoMode() != IoMode.WRITE ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_PERM, "an attempt to write without IO mode enabled");
            }

            long offset = _args.opwrite.offset.value.value;
            int count = _args.opwrite.data.remaining();

            RepositoryChannel fc = moverBridge.getFileChannel();

            if( offset + count > moverBridge.getAllocated() ) {
                moverBridge.getAllocator().allocate(INC_SPACE);
                moverBridge.setAllocated(moverBridge.getAllocated() + INC_SPACE);
            }
            _args.opwrite.data.rewind();
            int bytesWritten = fc.write(_args.opwrite.data, offset);

            moverBridge.getMover().setBytesTransferred(bytesWritten);

            res.status = nfsstat.NFS_OK;
            res.resok4 = new WRITE4resok();
            res.resok4.count = new count4( new uint32_t(bytesWritten) );
            res.resok4.committed = stable_how4.FILE_SYNC4;
            res.resok4.writeverf = new verifier4();
            res.resok4.writeverf.value = new byte[nfs4_prot.NFS4_VERIFIER_SIZE];

            _log.debug("MOVER: {}@{} written, {} requested.",
                    new Object[]{
                        bytesWritten,
                        offset,
                        bytesWritten
                    });

        }catch(ChimeraNFSException he) {
            _log.debug(he.getMessage());
            res.status = he.getStatus();
        }catch(IOException ioe) {
            _log.error("DSWRITE: ", ioe);
            res.status = nfsstat.NFSERR_IO;
        }catch(Exception e) {
            _log.error("DSWRITE: ", e);
            res.status = nfsstat.NFSERR_SERVERFAULT;
        }

       _result.opwrite = res;
        return _result;
    }
}