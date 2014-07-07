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
package org.dcache.cdmi.util;

import java.math.BigInteger;
import static org.dcache.cdmi.util.Crc16Calculator.doCRC;
import com.google.common.io.BaseEncoding;

/* This class converts between dCache's PnfsID and CDMI's ObjectID and the other way around.
*/

public class IdConverter
{

    private byte[] realObjectID = null;
    private String strObjectID = "";
    private String strPnfsID = "";
    private final byte reservedByte = 0;
    private static final int ENTERPRISE_NUMBER = 1343;  //see: http://www.iana.org/assignments/enterprise-numbers/enterprise-numbers
    private static final int ENTERPRISE_NUMBER_2 = 6840;  //alternative, see: http://www.iana.org/assignments/enterprise-numbers/enterprise-numbers
    private static final byte length = 40;

    /**
    * <p>
    * Converts a CDMI ObjectID to a dCache PnfsID.
    * </p>
     * @param objectID
     * @return
    */
    public String toPnfsID(String objectID)
    {
        strObjectID = objectID;
        realObjectID = fromBase16(strObjectID);
        byte[] pnfsIDArray = new byte[32];
        for (int i = 0; i < 32; i++) {
            int ind = 8 + i;
            pnfsIDArray[i] = realObjectID[ind];
        }
        String tmpPnfsID = fromNetworkByteOrder32(pnfsIDArray);
        if (tmpPnfsID.length() > 36) {
            int diff = tmpPnfsID.length() - 36;
            strPnfsID = tmpPnfsID.substring(diff);
        } else {
            strPnfsID = tmpPnfsID;
        }
        return strPnfsID;
    }

    /**
    * <p>
    * Converts a dCache PnfsID to a CDMI ObjectID.
    * realObjectID represents the ObjectID in reality, it isn't possible to display it as text since it is binary.
    * strObjectID represents the ObjectID converted to a Hex string so that in can be displayed as text (e.g. in JSON).
    * </p>
     * @param pnfsID
     * @return
    */
    public String toObjectID(String pnfsID)
    {
        strPnfsID = pnfsID;
        byte[] tempObjectID = new byte[40];
        // set 0th byte of tempObjectID
        tempObjectID[0] = reservedByte;
        // convert EnterpriseNumber to Network Byte Order, in exactly 3 bytes
        byte[] enterpriseNr = toNetworkByteOrder3(ENTERPRISE_NUMBER);
        // set 1st byte of tempObjectID
        tempObjectID[1] = enterpriseNr[0];
        // set 2nd byte of tempObjectID
        tempObjectID[2] = enterpriseNr[1];
        // set 3rd byte of tempObjectID
        tempObjectID[3] = enterpriseNr[2];
        // set 4th byte of tempObjectID
        tempObjectID[4] = reservedByte;
        // set 5th byte of tempObjectID
        tempObjectID[5] = length;
        // set 6th byte of tempObjectID
        tempObjectID[6] = reservedByte;
        // set 7th byte of tempObjectID
        tempObjectID[7] = reservedByte;
        // convert PnfsID to Network Byte Order, in exactly 32 bytes
        byte[] pnfsIDArray = toNetworkByteOrder32(strPnfsID);
        // set 8th - 39th byte of tempObjectID
        for (int i = 0; i < 32; i++) {
            int ind = 8 + i;
            tempObjectID[ind] = pnfsIDArray[i];
        }
        // calculate CRC-16 of tempObjectID and convert it to Network Byte Order, in exactly 2 bytes
        byte[] crc16Number = toNetworkByteOrder2(toCRC16(tempObjectID));
        // update 6th byte of tempObjectID with CRC-16 info
        tempObjectID[6] = crc16Number[0];
        // update 7th byte of tempObjectID with CRC-16 info
        tempObjectID[7] = crc16Number[1];
        // set realObjectID with info of tempObjectID
        realObjectID = tempObjectID;
        // encode realObjectID to stringObjectID with Base16
        strObjectID = toBase16(realObjectID);
        // return stringObjectID
        return strObjectID;
    }

    /**
    * <p>
    * Converts an integer to Network Byte Order with exactly 3 bytes (not more and not less bytes).
    * </p>
     * @param data
     * @return
    */
    private byte[] toNetworkByteOrder3(int data)
    {
        byte[] result = new byte[3];
        result[0] = (byte) ((data >> 16) & 0xff);
        result[1] = (byte) ((data >> 8) & 0xff);
        result[2] = (byte) (data & 0xff);
        return result;
    }

    /**
    * <p>
    * Converts a string to Network Byte Order with exactly 2 bytes (not more and not less bytes).
    * </p>
     * @param data
     * @return
    */
    private byte[] toNetworkByteOrder2(String data)
    {
        byte[] result = new byte[2];
        result[0] = (byte) ((data.charAt(0) >> 8) & 0xff);
        result[1] = (byte) (data.charAt(1) & 0xff);
        return result;
    }

    /**
    * <p>
    * Converts a string to Network Byte Order with exactly 32 bytes.
    * </p>
     * @param data
     * @return
    */
    private byte[] toNetworkByteOrder32(String data)
    {
        byte[] result = new byte[32];
        byte[] temp = new BigInteger(data, 16).toByteArray();
        if (temp.length < 33) {
            int diff = 32 - temp.length;
            for (int i = 0; i < diff; i++) {
                result[i] = 0;
            }
            for (int i = diff; i < 32; i++) {
                int ind = i - diff;
                result[i] = temp[ind];
            }
        }
        return result;
    }

    /**
    * <p>
    * Converts a byte array from Network Byte Order with exactly 32 bytes.
    * </p>
     * @param data
     * @return
    */
    private String fromNetworkByteOrder32(byte[] data)
    {
        String result = BaseEncoding.base16().encode(data);
        return result.toUpperCase();
    }

    /**
    * <p>
    * Calculates the CRC-16 for the CDMI ObjectID regarding CDMI specification from ISO/IEC.
    * </p>
     * @param data
     * @return
    */
    private String toCRC16(byte[] data)
    {
        String result = Long.toHexString(doCRC(data, 0x8005, 16, 0, true, true, false));
        return result.toUpperCase();
    }

    /**
    * <p>
    * Converts a byte array to a Base16 string (Hex).
    * </p>
     * @param data
     * @return
    */
    private String toBase16(byte[] data)
    {
        String result = BaseEncoding.base16().encode(data);
        return result.toUpperCase();
    }

    /**
    * <p>
    * Converts a Base16 string (Hex) to a byte array.
    * </p>
     * @param data
     * @return
    */
    private byte[] fromBase16(String data)
    {
        byte[] result = BaseEncoding.base16().decode(data);
        return result;
    }

}
