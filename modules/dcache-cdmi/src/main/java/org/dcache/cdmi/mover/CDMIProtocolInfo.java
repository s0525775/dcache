/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.cdmi.mover;

import java.net.InetSocketAddress;

import diskCacheV111.vehicles.IpProtocolInfo;

public class CDMIProtocolInfo implements IpProtocolInfo
{
    private final InetSocketAddress address;

    public CDMIProtocolInfo(InetSocketAddress address)
    {
        this.address = address;
    }

    @Override
    public String getProtocol()
    {
        return "CDMI";
    }

    @Override
    public int getMinorVersion()
    {
        return 0;
    }

    @Override
    public int getMajorVersion()
    {
        return 1;
    }

    @Override
    public String getVersionString()
    {
        return "CDMI-1.0";
    }

    @Override
    public InetSocketAddress getSocketAddress()
    {
        return address;
    }

}