/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package org.dcache.chimera.nfs.v4;

import org.dcache.chimera.nfs.nfsstat;
import org.dcache.chimera.nfs.v4.xdr.nfs_argop4;
import org.dcache.chimera.nfs.v4.xdr.nfs_opnum4;
import org.dcache.chimera.nfs.v4.xdr.LOOKUP4res;
import org.dcache.chimera.nfs.ChimeraNFSException;
import org.dcache.chimera.FileNotFoundHimeraFsException;
import org.dcache.chimera.FsInode;
import org.dcache.chimera.nfs.v4.xdr.nfs_resop4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperationLOOKUP extends AbstractNFSv4Operation {


        private static final Logger _log = LoggerFactory.getLogger(OperationLOOKUP.class);

	OperationLOOKUP(nfs_argop4 args) {
		super(args, nfs_opnum4.OP_LOOKUP);
	}

	@Override
	public nfs_resop4 process(CompoundContext context) {
        LOOKUP4res res = new LOOKUP4res();

        try {

            String name = NameFilter.convert(_args.oplookup.objname.value.value.value);

            if( context.currentInode().isLink() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_SYMLINK, "parent not a symbolic link");
            }

        	if( !context.currentInode().isDirectory() ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_NOTDIR, "parent not a directory");
        	}

            if (name.length() < 1 ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_INVAL, "invalid path");
            }

            if( name.length() > NFSv4Defaults.NFS4_MAXFILENAME ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_NAMETOOLONG, "path too long");
            }

            if( name.equals(".") || name.equals("..") ) {
                throw new ChimeraNFSException(nfsstat.NFSERR_BADNAME, "bad name '.' or '..'");
            }

            FsInode newInode = context.currentInode().inodeOf(name);
            context.currentInode(newInode);
            res.status = nfsstat.NFS_OK;

        }catch(FileNotFoundHimeraFsException he) {
        	res.status = nfsstat.NFSERR_NOENT;
        }catch(ChimeraNFSException he) {
            res.status = he.getStatus();
        }catch(Exception e) {
            _log.error("Error: ", e);
        	res.status = nfsstat.NFSERR_RESOURCE;
        }

       _result.oplookup = res;
            return _result;
	}

}