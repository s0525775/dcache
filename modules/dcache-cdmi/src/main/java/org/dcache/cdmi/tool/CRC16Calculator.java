package org.dcache.cdmi.tool;

//https://www.ssucet.org/pluginfile.php/1688/mod_assignment/intro/crc.java
/*
    Name       Poly     pbits  Init S   refIn   reflOut   xorOut   CRC for 123456789
    "CRC-16",  0x8005,  16,    0x0000,      1,        1,       0,  0xbb3d
*/

public class CRC16Calculator
{

    public static long doCRC(byte[] data, long poly, int pbits, long initS, boolean lsbfirst, boolean reflect, boolean xor)
    {
        long sreg = initS;
        long mask;
        if (pbits < 64)
            mask = (1L << pbits) - 1;
        else
            mask = ~0L;

        for (int i = 0; i < data.length; i++) {
            int b = data[i];
            for (int j = 0; j < 8; ++j) {
                int bit;
                if (!lsbfirst)
                    bit = ((b & (128>>j)) != 0 ) ? 1 : 0;
                else
                    bit = ((b & (1<<j)) != 0 ) ? 1 : 0;

                //only need one bit
                int hibit = (int)(sreg >> (pbits - 1)) & 1;

                sreg <<= 1;
                sreg |= bit;
                if( hibit != 0 )
                    sreg ^= poly;
                //System.out.printf("Bit: %d sreg=%x hi=%d\n",bit,sreg,hibit);
            }
        }
        for (int i = 0; i < pbits; ++i) {
            int hibit = (int)(sreg >> (pbits - 1)) & 1;   //guaranteed to fit in 1 bit
            sreg <<= 1;
            if (hibit != 0) {
                sreg ^= poly;
            }
        }
        if (reflect) {
            long refl = 0;
            long tmp = sreg;
            for(int j = 0; j < pbits; ++j){
                refl <<= 1;
                refl |= (tmp & 1);
                tmp >>= 1;
            }
            sreg = refl;
        }
        if (xor) {
            sreg ^= (1L << pbits) - 1;
        }
        return sreg & mask;
    }

}