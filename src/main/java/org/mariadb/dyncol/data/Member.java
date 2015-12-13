package org.mariadb.dyncol.data;

import java.sql.SQLException;

/*
MariaDB Dynamic column java plugin
Copyright (c) 2016 MariaDB.
...
...
 */
public class Member {

    private final DynamicType type;
    private final String key;
    private final byte[] rawBytes;

    /**
     * Member constructor, to represent dynamic member as object.
     * @param type one of the DynamicType
     * @param key member key (can be a named key, or an integer string representation)
     * @param rawBytes binary value of this object.
     */
    public Member(final DynamicType type, final String key, final byte[] rawBytes) {
        this.type = type;
        this.key = key;
        this.rawBytes = rawBytes;
    }

    /**
     * Get int value if data type is correct.
     * @return the int value
     * @throws SQLException if data type does not permit to calculate int value, or if a data truncation occur
     */
    public int getInt() throws SQLException {
        long value;
        int rawLength = rawBytes.length;
        switch (type) {
            case INT:
                value = ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? (rawBytes[1] & 0xff) << 8 : 0)
                        | ((rawLength > 2) ? (rawBytes[2] & 0xff) << 16 : 0)
                        | ((rawLength > 3) ? (rawBytes[3] & 0xff) << 24 : 0));
                //return value directly to avoid range check
                return (int) value;
            case UINT:
                value = ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? (rawBytes[1] & 0xff) << 8 : 0)
                        | ((rawLength > 2) ? (rawBytes[2] & 0xff) << 16 : 0)
                        | ((rawLength > 3) ? (rawBytes[3] & 0xff) << 24 : 0))
                        & 0xffffffffL;
                break;
            case DOUBLE:
                value = ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? ((long) (rawBytes[1] & 0xff) << 8) : 0)
                        | ((rawLength > 2) ? ((long) (rawBytes[2] & 0xff) << 16) : 0)
                        | ((rawLength > 3) ? ((long) (rawBytes[3] & 0xff) << 24) : 0)
                        | ((rawLength > 4) ? ((long) (rawBytes[4] & 0xff) << 32) : 0)
                        | ((rawLength > 5) ? ((long) (rawBytes[5] & 0xff) << 40) : 0)
                        | ((rawLength > 6) ? ((long) (rawBytes[6] & 0xff) << 48) : 0)
                        | ((rawLength > 7) ? ((long) (rawBytes[7] & 0xff) << 56) : 0));
                Double doubleValue = Double.longBitsToDouble(value);
                doubleValue.longValue();
                break;
            case STRING:
                value = 0;
                boolean neg = rawBytes[0] == '-';
                for (int i = neg ? 1 : 0 ; i < rawLength ; i++) {
                    value = value * 10 + rawBytes[i] - '0';
                }
                break;
            default:
                throw new SQLException("Invalid parameter type : asking fpr getInt on a " + type + " type", "HY105");
        }
        rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value);
        return (int) value;
    }

    /**
     * Get a long value if data type is correct.
     * @return the long value
     * @throws SQLException if data type does not permit to calculate int value, or if a data truncation occur
     */
    public long getUInt() throws SQLException {
        int rawLength = rawBytes.length;
        switch (type) {
            case INT:
                return ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? (rawBytes[1] & 0xff) << 8 : 0)
                        | ((rawLength > 2) ? (rawBytes[2] & 0xff) << 16 : 0)
                        | ((rawLength > 3) ? (rawBytes[3] & 0xff) << 24 : 0));
            case UINT:
                return ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? (rawBytes[1] & 0xff) << 8 : 0)
                        | ((rawLength > 2) ? (rawBytes[2] & 0xff) << 16 : 0)
                        | ((rawLength > 3) ? (rawBytes[3] & 0xff) << 24 : 0))
                        & 0xffffffffL;
            case STRING:
                long value = 0;
                boolean neg = rawBytes[0] == '-';
                for (int i = neg ? 1 : 0 ; i < rawLength ; i++) {
                    value = value * 10 + rawBytes[i] - '0';
                }
                rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value);
                return value;
            default:
                throw new SQLException("Invalid parameter type : asking fpr getInt on a " + type + " type", "HY105");
        }
    }

    /**
     * Get a double value if data type is correct.
     * @return the double value
     * @throws SQLException if data type does not permit to calculate int value, or if a data truncation occur
     */
    public double getDouble() throws SQLException {
        long value;
        int rawLength = rawBytes.length;
        switch (type) {
            case INT:
                return getInt();
            case UINT:
                return getUInt();
            case DOUBLE:
                value = ( ((rawLength > 0) ? (rawBytes[0] & 0xff) : 0 )
                        | ((rawLength > 1) ? ((long) (rawBytes[1] & 0xff) << 8) : 0)
                        | ((rawLength > 2) ? ((long) (rawBytes[2] & 0xff) << 16) : 0)
                        | ((rawLength > 3) ? ((long) (rawBytes[3] & 0xff) << 24) : 0)
                        | ((rawLength > 4) ? ((long) (rawBytes[4] & 0xff) << 32) : 0)
                        | ((rawLength > 5) ? ((long) (rawBytes[5] & 0xff) << 40) : 0)
                        | ((rawLength > 6) ? ((long) (rawBytes[6] & 0xff) << 48) : 0)
                        | ((rawLength > 7) ? ((long) (rawBytes[7] & 0xff) << 56) : 0));
                return Double.longBitsToDouble(value);
            case STRING:
                value = 0;
                boolean neg = rawBytes[0] == '-';
                for (int i = neg ? 1 : 0 ; i < rawLength ; i++) {
                    value = value * 10 + rawBytes[i] - '0';
                }
                break;
            default:
                throw new SQLException("Invalid parameter type : asking fpr getInt on a " + type + " type", "HY105");
        }
        rangeCheck(Integer.class, Integer.MIN_VALUE, Integer.MAX_VALUE, value);
        return (int) value;
    }


    /**
     * Check that value will not be truncated. Will throw an exception if truncation.
     * @param className Numeric class name
     * @param minValue the minimal value
     * @param maxValue the maximal value
     * @param value the current value
     * @throws SQLException if value is not inside bound
     */
    private void rangeCheck(Object className, long minValue, long maxValue, long value) throws SQLException {
        if (value < minValue || value > maxValue) {
            throw new SQLException("Out of range value for member with key = '" + key + "' : value " + value + " is not in "
                    + className + " range", "22003", 1264);
        }
    }

}
