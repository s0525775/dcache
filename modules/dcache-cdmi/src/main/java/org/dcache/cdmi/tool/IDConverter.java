/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.cdmi.tool;

import java.math.BigInteger;
import static org.dcache.cdmi.tool.CRC16Calculator.doCRC;
import static org.dcache.cdmi.tool.Base16Coder.*;

/**
 *
 * @author Jana
 */
public class IDConverter {

    private byte[] realObjectID = null;
    private String strObjectID = "";
    private String strPnfsID = "";
    private final byte reservedByte = 0;
    private final int enterpriseNumber = 1343;
    private final int enterpriseNumber2 = 6840;
    private final byte length = 40;

    public String toPnfsID(String objectID) {
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

    public String toObjectID(String pnfsID) {
        strPnfsID = pnfsID;
        byte[] tempObjectID = new byte[40];
        // set 0th byte of tempObjectID
        tempObjectID[0] = reservedByte;
        // convert EnterpriseNumber to Network Byte Order
        byte[] enterpriseNr = toNetworkByteOrder3(enterpriseNumber);
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
        // set convert PnfsID to Network Byte Order
        byte[] pnfsIDArray = toNetworkByteOrder32(strPnfsID);
        // set 8th - 39th byte of tempObjectID
        for (int i = 0; i < 32; i++) {
            int ind = 8 + i;
            tempObjectID[ind] = pnfsIDArray[i];
        }
        // calculate CRC-16 of tempObjectID and convert it to Network Byte Order
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

    private byte[] toNetworkByteOrder3(int data) {
        byte[] result = new byte[3];
        result[0] = (byte) ((data >> 16) & 0xff);
        result[1] = (byte) ((data >> 8) & 0xff);
        result[2] = (byte) (data & 0xff);
        return result;
    }

    private byte[] toNetworkByteOrder2(String data) {
        byte[] result = new byte[2];
        result[0] = (byte) ((data.charAt(0) >> 8) & 0xff);
        result[1] = (byte) (data.charAt(1) & 0xff);
        return result;
    }

    private byte[] toNetworkByteOrder32(String data) {
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

    private String fromNetworkByteOrder32(byte[] data) {
        String result = encode(data);
        return result.toUpperCase();
    }

    private String toCRC16(byte[] data) {
        String result = Long.toHexString(doCRC(data, 0x8005, 16, 0, true, true, false));
        return result.toUpperCase();
    }

    private String toBase16(byte[] data) {
        String result = encode(data);
        return result.toUpperCase();
    }

    private byte[] fromBase16(String data) {
        byte[] result = decode(data);
        return result;
    }

    /*
    public void test() {
        String oldID0 = "00007CE39F587D004C57BF7BF822257B35EB";
        System.out.println("PnfsID: " + oldID0 + " | " + oldID0.length());
        String newID1 = new IDConverter().toObjectID(oldID0);
        System.out.println("ObjectID: " + newID1 + " | " + newID1.length());
        String newID2 = new IDConverter().toPnfsID(newID1);
        System.out.println("PnfsID2: " + newID2 + " | " + newID2.length());
    }
    */

}