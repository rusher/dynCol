package org.mariadb.dyncol;

import org.junit.Assert;
import org.junit.Test;
import org.mariadb.dyncol.data.DynamicTypee;
import org.mariadb.dyncol.data.Member;

import java.sql.SQLException;

public class MemberTest {

    @Test
    public void intOneByteArrayTest() throws SQLException {
        byte[] oneByteValue = new byte[1];
        oneByteValue[0] = 0x01;
        Member member = new Member(DynamicTypee.INT, "name", oneByteValue);
        Assert.assertEquals(1, member.getInt());
    }

    @Test
    public void intTwoByteArrayTest() throws SQLException {
        byte[] oneByteValue = new byte[2];
        oneByteValue[0] = 0x00;
        oneByteValue[1] = 0x01;
        Member member = new Member(DynamicTypee.INT, "name", oneByteValue);
        Assert.assertEquals(256, member.getInt());
    }

    @Test
    public void intThreeByteArrayTest() throws SQLException {
        byte[] oneByteValue = new byte[3];
        oneByteValue[0] = 0x00;
        oneByteValue[1] = 0x00;
        oneByteValue[2] = 0x01;
        Member member = new Member(DynamicTypee.INT, "name", oneByteValue);
        Assert.assertEquals(65536, member.getInt());
    }

    @Test
    public void intFourByteArrayTest() throws SQLException {
        byte[] oneByteValue = new byte[4];
        oneByteValue[0] = (byte) 0xff;
        oneByteValue[1] = (byte) 0xff;
        oneByteValue[2] = (byte) 0xff;
        oneByteValue[3] = (byte) 0x7f;
        Member member = new Member(DynamicTypee.INT, "name", oneByteValue);
        Assert.assertEquals(Integer.MAX_VALUE, member.getInt());
    }

    @Test
    public void intFourByteArrayNegativeTest() throws SQLException {
        byte[] oneByteValue = new byte[4];
        oneByteValue[0] = (byte) 0x00;
        oneByteValue[1] = (byte) 0x00;
        oneByteValue[2] = (byte) 0x00;
        oneByteValue[3] = (byte) 0x80;
        Member member = new Member(DynamicTypee.INT, "name", oneByteValue);
        Assert.assertEquals(Integer.MIN_VALUE, member.getInt());
    }

    @Test
    public void intUnsignedFourByteArrayTest() throws SQLException {
        byte[] oneByteValue = new byte[4];
        oneByteValue[0] = (byte) 0x00;
        oneByteValue[1] = (byte) 0x00;
        oneByteValue[2] = (byte) 0x00;
        oneByteValue[3] = (byte) 0x80;
        Member member = new Member(DynamicTypee.UINT, "name", oneByteValue);
        Assert.assertEquals(Integer.MAX_VALUE + 1L, member.getUInt());
    }

    @Test
    public void intFromLongValidTest() throws SQLException {
        byte[] oneByteValue = new byte[8];
        oneByteValue[0] = 0x00;
        oneByteValue[1] = 0x00;
        oneByteValue[2] = 0x00;
        oneByteValue[3] = 0x01;
        oneByteValue[4] = 0x00;
        oneByteValue[5] = 0x00;
        oneByteValue[6] = 0x00;
        oneByteValue[7] = 0x00;
        Member member = new Member(DynamicTypee.DOUBLE, "name", oneByteValue);
        Assert.assertEquals(16777216, member.getInt());
    }

    @Test(expected = SQLException.class)
    public void intFromLongNotValidTest() throws SQLException {
        byte[] oneByteValue = new byte[8];
        oneByteValue[0] = 0x00;
        oneByteValue[1] = 0x00;
        oneByteValue[2] = 0x00;
        oneByteValue[3] = 0x00;
        oneByteValue[4] = 0x00;
        oneByteValue[5] = 0x00;
        oneByteValue[6] = 0x00;
        oneByteValue[7] = 0x01;
        Member member = new Member(DynamicTypee.DOUBLE, "name", oneByteValue);
        member.getInt();
    }

    @Test
    public void intStringTest() throws SQLException {
        byte[] oneByteValue = "16777216".getBytes();
        Member member = new Member(DynamicTypee.STRING, "name", oneByteValue);
        Assert.assertEquals(16777216, member.getInt());
    }

    @Test(expected = SQLException.class)
    public void intStringMoreThanIntArrayTest() throws SQLException {
        byte[] oneByteValue = "167772168999999".getBytes();
        Member member = new Member(DynamicTypee.STRING, "name", oneByteValue);
        member.getInt();
    }

    @Test(expected = SQLException.class)
    public void intDateTimeExceptionTest() throws SQLException {
        Member member = new Member(DynamicTypee.DATETIME, "name", new byte[4]);
        member.getInt();
    }

    @Test(expected = SQLException.class)
    public void intDateExceptionTest() throws SQLException {
        Member member = new Member(DynamicTypee.DATE, "name", new byte[4]);
        member.getInt();
    }

    @Test(expected = SQLException.class)
    public void intDyncolExceptionTest() throws SQLException {
        Member member = new Member(DynamicTypee.DYNCOL, "name", new byte[4]);
        member.getInt();
    }

    @Test(expected = SQLException.class)
    public void intTimeExceptionTest() throws SQLException {
        Member member = new Member(DynamicTypee.TIME, "name", new byte[4]);
        member.getInt();
    }

}
