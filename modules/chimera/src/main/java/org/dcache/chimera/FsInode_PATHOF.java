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
package org.dcache.chimera;

import org.dcache.chimera.posix.Stat;

public class FsInode_PATHOF extends FsInode {

    private String _path;

    public FsInode_PATHOF(FileSystemProvider fs, String id) {
        super(fs, id, FsInodeType.PATHOF);
    }

    @Override
    public int read(long pos, byte[] data, int offset, int len) {

        if (_path == null) {
            try {
                _path = _fs.inode2path(this);
            } catch (ChimeraFsException e) {
                return -1;
            }
        }

        byte[] b = (_path + "\n").getBytes();
        /*
         * are we still inside ?
         */
        if (pos > b.length) {
            return 0;
        }

        int copyLen = Math.min(len, b.length - (int) pos);
        System.arraycopy(b, (int) pos, data, 0, copyLen);

        return copyLen;
    }

    @Override
    public Stat stat() throws ChimeraFsException {

        Stat ret = super.stat();
        ret.setMode((ret.getMode() & 0000777) | UnixPermission.S_IFREG);
        if (_path == null) {
            _path = _fs.inode2path(this);
        }

        ret.setSize(_path.length() + 1);
        return ret;
    }

    @Override
    public int write(long pos, byte[] data, int offset, int len) {
        return -1;
    }
}
