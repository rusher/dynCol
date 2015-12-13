package org.mariadb.dyncol;


import org.mariadb.dyncol.blob.Blob;
import org.mariadb.dyncol.data.DynamicType;
import org.mariadb.dyncol.data.Record;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class DynCol {
    private final static String ISO__ = "ISO-10646-UCS-2";

    Map<String, Record> data = new HashMap<>();
    private int column_count = 0;
    private boolean is_str_type = false; //true - Columns with names, false - Numeric format
    int error_index = 0;
    Blob blb = null;

    public void setInt(String name, long value) {
        data.put(name, new Record());
        get_str_type(name);
        Record r = data.get(name);
        r.record_type = 1;
        r.long_value = value;
        column_count = data.size();
    }

    public void setUint(String name, long value) {
        get_str_type(name);
        if (value < 0) {
            throw new IllegalArgumentException("" + value);
        } else {
            data.put(name, new Record());
            Record r = data.get(name);
            r.record_type = 2;
            r.long_value = value;
            column_count = data.size();
        }
    }

    public void setString(String name, String value) {
        if (value == null) {
            if (data.containsKey(name))
                data.remove(name);
        } else {
            get_str_type(name);
            data.put(name, new Record());
            Record r = data.get(name);
            r.record_type = 4;
            r.str_value = value;
            column_count = data.size();
        }
    }

    public void setDouble(String name, double value) {
        get_str_type(name);
        data.put(name, new Record());
        Record r = data.get(name);
        r.record_type = 3;
        r.double_value = value;
        column_count = data.size();
    }

    public void setDynCol(String name, DynCol value) throws Exception {
        if (value == null) {
            if (data.containsKey(name))
                data.remove(name);
        } else {
            DynCol tempd = new DynCol();
            tempd.setBlob(value.getBlob());
            get_str_type(name);
            data.put(name, new Record());
            Record r = data.get(name);
            r.record_type = 9;
            r.DynCol_value = tempd;
            column_count = data.size();
        }
    }

    public void remove(String name) {
        data.remove(name);
    }

    public long getInt(String name) throws Exception {
        Record r = data.get(name);
        if (r == null) {
            return 0;
        }
        switch (r.record_type) {
            case 1:
                return r.long_value;
            case 2:
                return r.long_value;
            case 3:
                return (long) r.double_value;
            case 4:
                return Long.parseLong(r.str_value);
            case 9:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            default:
                throw new Exception("ER_DYNCOL_FORMAT");
        }
    }

    public long getUint(String name) throws Exception {
        Record r = data.get(name);
        if (r == null) {
            return 0;
        }
        switch (r.record_type) {
            case 1:
                return r.long_value;
            case 2:
                return r.long_value;
            case 3:
                return (long) r.double_value;
            case 4:
                return Long.parseLong(r.str_value);
            case 9:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            default:
                throw new Exception("ER_DYNCOL_FORMAT");
        }
    }

    public String getString(String name) throws Exception {
        Record r = data.get(name);
        if (r == null) {
            return null;
        }
        switch (r.record_type) {
            case 1:
                return "" + r.long_value;
            case 2:
                return "" + r.long_value;
            case 3:
                return "" + r.double_value;
            case 4:
                return r.str_value;
            case 9:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            default:
                throw new Exception("ER_DYNCOL_FORMAT");
        }
    }

    public double getDouble(String name) throws SQLException {
        Record r = data.get(name);
        if (r == null) {
            throw new SQLException("Parameter '" + name + "' unknown", "07002");
        }
        switch (r.type) {
            case INT:
                return (double) r.long_value;
            case UINT:
                return (double) r.long_value;
            case DOUBLE:
                return r.double_value;
            case STRING:
                return Long.parseLong(r.str_value);
            default:
                throw new SQLException("Invalid parameter type : asking fpr getDouble on a " + r.type + " type", "HY105");
        }
    }

    public DynCol getDynCol(String name) throws Exception {
        Record r = data.get(name);
        if (r == null) {
            return null;
        }
        switch (r.record_type) {
            case 1:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            case 2:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            case 3:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            case 4:
                throw new Exception("ER_DYNCOL_TRUNCATED");
            case 9:
                return r.DynCol_value;
            default:
                throw new Exception("ER_DYNCOL_FORMAT");
        }
    }

    public int getRecodFormat(String name) {
        Record r = data.get(name);
        return r.record_type;
    }


    public void setJson(String s) throws Exception {
        if (s == null) {
            throw new NullPointerException();
        }
        data.clear();
        if (!parse_json(s, data)) {
            data.clear();
            throw new Exception("syntax error, symbol #" + error_index);
        }
    }

    public void addJson(String s) throws Exception {
        if (s == null) {
            throw new NullPointerException();
        }
        Map<String, Record> data = new HashMap<String, Record>();
        if (!parse_json(s, data)) {
            data.clear();
            throw new Exception("syntax error, symbol #" + error_index);
        } else {
            this.data.putAll(data);
        }
    }

    public String getJson() {
        String result = "{";
        if (data.size() == 0) {
            result = "{}";
            return result;
        }
        boolean is_first = true;
        for (Entry<String, Record> entry : data.entrySet()) {
            if (!is_first) {
                result += ',';
            } else {
                is_first = false;
            }
            result += "\"" + entry.getKey() + "\":";
            switch (entry.getValue().record_type) {
                case 0:
                    result += "null";
                    break;
                case 1:
                    result += entry.getValue().long_value;
                    break;
                case 2:
                    result += entry.getValue().long_value;
                    break;
                case 3:
                    result += entry.getValue().double_value;
                    break;
                case 4:
                    result += "\"" + entry.getValue().str_value + "\"";
                    break;
                case 9:
                    result += entry.getValue().DynCol_value.getJson();
                    break;
            }
        }
        result += '}';
        return result;
    }

    //Save blob
    public void setBlob(byte[] blob) throws Exception {
        if (blob == null) {
            throw new NullPointerException();
        } else if (blob.length == 3 || blob.length == 5) {
            data.clear();
        } else {
            error_index = 0;
            data.clear();
            blb = new Blob();
            blb.blob_size = blob.length;
            //if 2nd bit is 1, then type of string is named
            if (((blob[0] & 4) >>> 2) == 1)
                is_str_type = true;
            else
                is_str_type = false;
            //read the offset size from  first 2 bits
            blb.offset_size = (blob[0] & 3) + (is_str_type ? 2 : 1);
            //read the column count from 1st and 2nd bytes
            column_count = ((blob[2] & 0xFF) << 8) | (blob[1] & 0xFF);
            //calculating offset of all the header
            blb.header_offset = (is_str_type ? 5 : 3) + column_count * (2 + blb.offset_size);
            if (is_str_type) {
                //read name pool size from bytes #3 and 4
                blb.nmpool_size = ((blob[4] & 0xFF) << 8) | (blob[3] & 0xFF);
                String name;
                Record rc = new Record();
                int real_offset = 5,
                        next_offset_name = 0,
                        real_offset_name = 0,
                        next_offset_value = 0,
                        real_offset_value = 0;
                //read offset of the name, from beginning of name pool
                real_offset_name = (blob[real_offset] & 0xFF) | ((blob[real_offset + 1] & 0xFF) << 8);
                real_offset += 2;
                //read the record type
                rc.record_type = (blob[real_offset] & 15) + 1;
                //read offset of the value, from beginning of the data pool
                real_offset_value = (blob[real_offset] & 0xFF) >>> 4;
                real_offset++;
                if (blb.offset_size > 1) {
                    real_offset_value = real_offset_value | ((blob[real_offset] & 0xFF) << 4);
                    real_offset++;
                }
                for (int index = 2; index < blb.offset_size; index++) {
                    real_offset_value = real_offset_value | (blob[real_offset] & 0xFF) << (4 + 8 * (index - 1));
                    real_offset++;
                }
                for (int i = 0; i < column_count; i++) {
                    //the next record type
                    int record_type = 0;
                    //if it isn't the last column
                    if (i != column_count - 1) {
                        //reading the next name offset
                        next_offset_name = (blob[real_offset] & 0xFF) | ((blob[real_offset + 1] & 0xFF) << 8);
                        real_offset += 2;
                        //reading the next record type
                        record_type = (blob[real_offset] & 15) + 1;
                        //reading the next value offset
                        next_offset_value = (blob[real_offset] & 0xFF) >>> 4;
                        real_offset++;
                        if (blb.offset_size > 1) {
                            next_offset_value = next_offset_value | (blob[real_offset] & 0xFF) << (4);
                            real_offset++;
                        }
                        for (int index = 2; index < blb.offset_size; index++) {
                            next_offset_value = next_offset_value | (blob[real_offset] & 0xFF) << (4 + 8 * (index - 1));
                            real_offset++;
                        }
                    } else {
                        next_offset_name = blb.nmpool_size;
                        next_offset_value = blb.blob_size - blb.header_offset - blb.nmpool_size;
                    }
                    //calculating the name size
                    int byte_long = next_offset_name - real_offset_name;
                    //read the name
                    byte[] array_byte = new byte[byte_long];
                    for (int index = 0; index < byte_long; index++) {
                        array_byte[index] = blob[blb.header_offset + real_offset_name + index];
                    }
                    name = new String(array_byte, "UTF-8");
                    real_offset_name = next_offset_name;
                    //read  the values
                    switch (rc.record_type) {
                        case 1:
                            save_sint(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 2:
                            save_uint(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 3:
                            save_double(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 4:
                            save_Str(blob, rc, real_offset_value, next_offset_value - real_offset_value - 1);
                            break;
                        case 9:
                            save_DynCol(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        default:
                            throw new Exception("Wrong format");
                    }
                    data.put(name, rc);
                    real_offset_value = next_offset_value;
                    rc = new Record();
                    rc.record_type = record_type;
                }
            } else {
                String name, next_name;
                Record rc = new Record();
                int real_offset = 3,
                        next_offset_value = 0,
                        real_offset_value = 0;
                //read the name
                name = "" + ((blob[real_offset] & 0xFF) | ((blob[real_offset + 1] & 0xFF) << 8));
                real_offset += 2;
                //read the record type
                rc.record_type = (blob[real_offset] & 7) + 1;
                //read offset of the value, from beginning of the data pool
                real_offset_value = (blob[real_offset] & 0xFF) >>> 3;
                real_offset++;
                if (blb.offset_size > 1) {
                    real_offset_value = real_offset_value | ((blob[real_offset] & 0xFF) << 3);
                    real_offset++;
                }
                for (int index = 2; index < blb.offset_size; index++) {
                    real_offset_value = real_offset_value | (blob[real_offset] & 0xFF) << (3 + 8 * (index - 1));
                    real_offset++;
                }
                for (int i = 0; i < column_count; i++) {
                    //the next record type
                    int record_type = 0;
                    //if it isn't the last column
                    if (i != column_count - 1) {
                        next_name = "" + ((blob[real_offset] & 0xFF) | ((blob[real_offset + 1] & 0xFF) << 8));
                        real_offset += 2;
                        //reading the next record type
                        record_type = (blob[real_offset] & 7) + 1;
                        //reading the next value offset
                        next_offset_value = (blob[real_offset] & 0xFF) >>> 3;
                        real_offset++;
                        if (blb.offset_size > 1) {
                            next_offset_value = next_offset_value | (blob[real_offset] & 0xFF) << (3);
                            real_offset++;
                        }
                        for (int index = 2; index < blb.offset_size; index++) {
                            next_offset_value = next_offset_value | (blob[real_offset] & 0xFF) << (3 + 8 * (index - 1));
                            real_offset++;
                        }
                    } else {
                        next_name = null;
                        next_offset_value = blb.blob_size - blb.header_offset;
                    }
                    //read  the values
                    switch (rc.record_type) {
                        case 1:
                            save_sint(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 2:
                            save_uint(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 3:
                            save_double(blob, rc, real_offset_value, next_offset_value - real_offset_value);
                            break;
                        case 4:
                            save_Str(blob, rc, real_offset_value, next_offset_value - real_offset_value - 1);
                            break;
                        default:
                            throw new Exception("Wrong format");
                    }
                    data.put(name, rc);
                    name = next_name;
                    real_offset_value = next_offset_value;
                    rc = new Record();
                    rc.record_type = record_type;
                }
            }
        }
    }

    public byte[] getBlob() throws Exception {
        byte[] Blob;
        if (data.size() == 0) {
            Blob = new byte[3];
            return Blob;
        }
        int blb_size = get_blob_length();
        Blob = new byte[(blb.blob_size = blb_size)];
        //put the flags
        Blob[0] = (byte) (((blb.offset_size - (is_str_type ? 2 : 1)) & 3) | (is_str_type ? 4 : 0));
        //put column count
        Blob[1] = (byte) (column_count & 0xFF);
        Blob[2] = (byte) ((column_count >>> 8) & 0xFF);
        //offset of
        /* offset is offset of header entry
		 * header entry is column number or string pointer + offset & type */
        int offset = 0;
        //offset from beginning of the data pool
        int data_offset = 0;
        if (is_str_type) {
            String[] keyarray = new String[column_count];
            //put the length of name pool
            Blob[3] = (byte) (blb.nmpool_size & 0xFF);
            Blob[4] = (byte) ((blb.nmpool_size >>> 8) & 0xFF);
            int i = 0;
            for (String key : data.keySet()) {
                keyarray[i] = key;
                i++;
            }
            //sort the names
            qSort(keyarray, 0, column_count - 1);
            offset = 5;//flag byte + number of columns 2 bytes + name pool length 2 bytes = 5 bytes
            //offset from beginning of the name pool
            int name_offset = 0;
            for (i = 0; i < column_count; i++) {
                Record rec = data.get("" + keyarray[i]);
                //put offset of the name
                Blob[offset] = (byte) (name_offset & 0xFF);
                Blob[offset + 1] = (byte) ((name_offset >>> 8) & 0xFF);
                offset += 2;
                //put offset of the data + 4 byte data type
                Blob[offset] = (byte) ((rec.record_type - 1) | ((data_offset & 15) << 4));
                int tdata_offset = data_offset >>> 4;
                for (int j = 1; j < blb.offset_size; j++) {
                    Blob[offset + j] = (byte) ((tdata_offset & 0xFF));
                    tdata_offset = data_offset >>> 8;
                }
                offset += blb.offset_size;
                //put the name
                byte[] name = keyarray[i].getBytes();
                for (int ind = 0; ind < name.length; ind++) {
                    Blob[blb.header_offset + name_offset + ind] = name[ind];
                }
                name_offset += name.length;
                //put the data
                switch (rec.record_type) {
                    case 0:
                        throw new Exception("Wrong format");
                    case 1:
                        data_offset += put_sint(Blob, rec.long_value, (blb.header_offset + blb.nmpool_size + data_offset));
                        break;
                    case 2:
                        data_offset += put_uint(Blob, rec.long_value, (blb.header_offset + blb.nmpool_size + data_offset));
                        break;
                    case 3:
                        data_offset += put_double(Blob, rec.double_value, blb.header_offset + blb.nmpool_size + data_offset);
                        break;
                    case 4:
                        data_offset += put_str(Blob, rec.str_value, blb.header_offset + blb.nmpool_size + data_offset);
                        break;
                    case 9:
                        data_offset += put_DynCol(Blob, rec.DynCol_value.getBlob(), blb.header_offset + blb.nmpool_size + data_offset);
                        break;
                }
            }
        } else {
            int[] keyarray = new int[column_count];
            int i = 0;
            for (String key : data.keySet()) {
                keyarray[i] = Integer.parseInt(key);
                i++;
            }
            //sort the names
            qSort(keyarray, 0, column_count - 1);
            offset = 3;//flag byte + number of columns 2 bytes = 3 bytes
            for (i = 0; i < column_count; i++) {
                Record rec = data.get("" + keyarray[i]);
                //put the name
                Blob[offset] = (byte) (keyarray[i] & 0xFF);
                Blob[offset + 1] = (byte) ((keyarray[i] >>> 8) & 0xFF);
                offset += 2;
                //put offset of the data + 3 byte data type
                Blob[offset] = (byte) ((rec.record_type - 1) | ((data_offset & 31) << 3));
                int tdata_offset = data_offset >>> 5;
                for (int j = 1; j < blb.offset_size; j++) {
                    Blob[offset + j] = (byte) ((tdata_offset & 0xFF));
                    tdata_offset = data_offset >>> 8;
                }
                offset += blb.offset_size;
                //put the data
                switch (rec.record_type) {
                    case 0:
                        throw new Exception("Wrong format");
                    case 1:
                        data_offset += put_sint(Blob, rec.long_value, (blb.header_offset + data_offset));
                        break;
                    case 2:
                        data_offset += put_uint(Blob, rec.long_value, (blb.header_offset + data_offset));
                        break;
                    case 3:
                        data_offset += put_double(Blob, rec.double_value, blb.header_offset + data_offset);
                        break;
                    case 4:
                        data_offset += put_str(Blob, rec.str_value, blb.header_offset + data_offset);
                        break;
                }
            }
        }
        return Blob;
    }
    //Not user's methods

    //depends on name change type of Blob
    private void get_str_type(String name) {
        if (!is_num(name)) {
            is_str_type = true;
        }
    }

    /*Check the Json, Save Json
     * It use Finite-state machine
     * s - Json string
     * data - where Json will be saved
     */
    private boolean parse_json(String s, Map<String, Record> data) {
        boolean result = false;
        int test_table[][] = {
                {0, 1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1},
                {1, -1, -1, 2, -1, -1, 7, -1, -1, -1, -1, -1},
                {2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 2},
                {3, -1, -1, -1, 4, -1, -1, -1, -1, -1, -1, -1},
                {4, 1, 5, 8, -1, -1, -1, -1, -1, 10, 6, -1},
                {6, -1, 5, -1, -1, -1, 7, 1, -1, -1, -1, 5},
                {6, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1},
                {7, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1},
                {8, 8, 8, 9, 8, 8, 8, 8, 8, 8, 8, 8},
                {9, -1, -1, -1, -1, -1, 7, 1, -1, -1, -1, -1},
                {-1, -1, 5, -1, -1, -1, 7, 1, -1, -1, -1, -1},
        };
        char c;
        int x = 0, y = 0;
        boolean nested = false;
        String string = "";
        String name = "";
        int braces = 0;
        for (int i = 0; i < s.length(); i++) {
            c = s.charAt(i);
            switch (c) {
                case '{':
                    x = 1;
                    break;
                case '"':
                    x = 3;
                    break;
                case ':':
                    x = 4;
                    break;
                case '\\':
                    x = 5;
                    break;
                case '}':
                    x = 6;
                    break;
                case ',':
                    x = 7;
                    break;
                case '-':
                    x = 9;
                    break;
                case '.':
                    x = 11;
                    break;
                case 'n':
                    if (s.substring(i, i + 4).equals("null")) {
                        x = 10;
                        i += 3;
                    } else x = 8;
                    break;
                default:
                    if (c == ' ' || c == '	') {
                        x = 0;
                    } else if (c >= '0' && c <= '9') {
                        x = 2;
                    } else {
                        x = 8;
                    }
                    break;
            }
            if (test_table[y][x] < 0) {
                error_index = i;
                return false;
            }
            //if nested Json
            else if (y == 4 && x == 1) {
                nested = true;
                is_str_type = true;
                braces++;
                int coma = s.indexOf(",", i);
                if (coma < 0)
                    coma = s.lastIndexOf("}");
                int end = s.lastIndexOf('}', coma - 1);
                string = s.substring(i, end + 1);
                Record r = data.get(name);
                r.record_type = 9;
                r.DynCol_value = new DynCol();
                try {
                    r.DynCol_value.setJson(string);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                i = end;
                y = 7;
                x = 6;
            }
            //if string continuing
            if (y == 7)
                result = false;
                //if inside the name characters
            else if (((y == 2 && (x == 0 || x == 1 || x == 4 || x == 5 || x == 6 || x == 7 || x == 8 || x == 9 || x == 10 || x == 11)))
                    && (!nested))
                is_str_type = true;
            //if inside the string back slash
            if ((y == 2 || y == 8) && x == 5)
                i++;
            //if nested string is ended
            if (x == 6 && (y == 1 || y == 5 || y == 6 || y == 7 || y == 9)) {
                braces--;
                result = true;
                nested = false;
            }
            //if new name starting, clear the name string
            if (y == 1 && x == 3) {
                name = "";
            }
            //if inside the string name back slash, write next symbol
            else if ((y == 2) && x == 5) {
                name = name + s.charAt(i);
            }
            //if string name ended, save name
            else if ((y == 2) && x == 3) {
                data.put(name, new Record());
            }
            //
            else if (y == 2) {
                name = name + c;
            }
            //if character is ':' clear value string
            else if (y == 3 && x == 4) {
                string = "";
            }
            //if value string ended save it
            else if (y == 8 && x == 3) {
                Record r = data.get(name);
                r.record_type = 4;
                r.str_value = string;
            }
            //if inside the value string back slash, write next symbol
            else if (y == 8 && x == 5) {
                string = string + s.charAt(i);
            }
            //Just write value string
            else if (y == 8) {
                string = string + c;
            }
            //if first symbol of value is numeral
            else if (y == 4 && x == 2) {
                data.get(name).record_type = 2;
                string += c;
            }
            //if first symbol of value is minus
            else if (y == 4 && x == 9) {
                data.get(name).record_type = 1;
                string += c;
            }
            //if numeral continue
            else if ((y == 5 || y == 10) && x == 2) {
                string += c;
            }
            //if punkt
            else if (y == 5 && x == 11) {
                data.get(name).record_type = 3;
                string += c;
            }
            //if number is ended, save it
            else if (y == 5) {
                Record r = data.get(name);
                //if double
                if (r.record_type == 3) {
                    r.double_value = Double.parseDouble(string);
                } else {
                    r.long_value = Long.parseLong(string);
                }
            }
            //if null, delete column
            else if (y == 4 && x == 10) {
                data.remove(name);
            }
            y = test_table[y][x];

        }
        //because first '{' was not reading
        braces++;
        if (!result || braces != 0) {
            result = false;
        }
        column_count = data.size();
        return result;
    }

    //Check is string s number
    private boolean is_num(String s) {
        if ((s.charAt(0) == '-' && s.length() > 1) || !(s.charAt(0) >= '0' && s.charAt(0) <= '9')) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            if (!(s.charAt(i) >= '0' && s.charAt(i) <= '9')) return false;
        }
        return true;
    }

    //Check Blob length
    private int get_blob_length() throws Exception {
        int res = 0;
        blb = new Blob();
        for (String key : data.keySet()) {
            Record rec = data.get(key);
            switch (rec.record_type) {
                case 0:
                    break;
                case 1:
                    res += dynamic_column_sint_bytes(rec.long_value);
                    break;
                case 2:
                    res += dynamic_column_uint_bytes(rec.long_value);
                    break;
                case 3:
                    res += 8;
                    break;
                case 4:
                    res += (dynamic_column_sint_bytes(45/*UTF-8*/) + rec.str_value.getBytes().length);
                    break;
                case 9:
                    res += rec.DynCol_value.get_blob_length();
                    break;
            }
            if (is_str_type)
                blb.nmpool_size += key.getBytes().length;
        }
        blb.offset_size = dynamic_column_offset_bytes(res);
        if (is_str_type)
            blb.header_offset = column_count * (2/*2 bytes offset from the name pool*/ + blb.offset_size) + 5;/*
		  length of fixed string header with names
		  1 byte - flags, 2 bytes - columns counter,  2 bytes - name pool size
		*/
        else
            blb.header_offset = column_count * (2/*2 bytes offset from the name pool*/ + blb.offset_size) + 3;// length of fixed string header 1 byte - flags, 2 bytes - columns counter
        res += blb.nmpool_size;
        res += blb.header_offset;
        return res;
    }

    //Calculating offset size
    private int dynamic_column_offset_bytes(int data_length) throws Exception {
        if (is_str_type) {
            if (data_length < 0xfff)                /* all 1 value is reserved */
                return 2;
            if (data_length < 0xfffff)              /* all 1 value is reserved */
                return 3;
            if (data_length < 0xfffffff)            /* all 1 value is reserved */
                return 4;
            //without if (data_length < 0xfffffffff), because of max array index is signed int
            throw new Exception("ER_DYNCOL_UNSUPPORTED_DATA_LENGTH");
        } else {
            if (data_length < 0x1f)                /* all 1 value is reserved */
                return 1;
            if (data_length < 0x1fff)              /* all 1 value is reserved */
                return 2;
            if (data_length < 0x1fffff)            /* all 1 value is reserved */
                return 3;
            if (data_length < 0x1fffffff)          /* all 1 value is reserved */
                return 4;
            throw new Exception("ER_DYNCOL_UNSUPPORTED_DATA_LENGTH");
        }
    }

    //Calculating uint size
    private int dynamic_column_uint_bytes(long val) {
        int len;
        for (len = 0; val != 0; val >>>= 8, len++)
            ;
        return len;
    }

    // Calculating int size
    private int dynamic_column_sint_bytes(long val) {
        return dynamic_column_uint_bytes((val << 1) ^
                (val < 0 ? (-1) : 0));
    }

    /*
     * a - array that should be sorting
     * low - index of first element
     * high - index of last element
     */
    private static void qSort(int[] a, int low, int high) {
        int i = low;
        int j = high;
        int x = a[(low + high) / 2];
        do {
            while (a[i] < x) ++i;
            while (a[j] > x) --j;
            if (i <= j) {
                int tmp = a[i];
                a[i] = a[j];
                a[j] = tmp;
                i++;
                j--;
            }
        } while (i <= j);
        if (low < j) qSort(a, low, j);
        if (i < high) qSort(a, i, high);
    }

    private static void qSort(String[] a, int low, int high) {
        int i = low;
        int j = high;
        String x = a[(low + high) / 2];
        do {
            while (compareStr(a[i], x) < 0) ++i;
            while (compareStr(a[j], x) > 0) --j;
            if (i <= j) {
                String tmp = a[i];
                a[i] = a[j];
                a[j] = tmp;
                i++;
                j--;
            }
        } while (i <= j);
        if (low < j) qSort(a, low, j);
        if (i < high) qSort(a, i, high);
    }

    private static int compareStr(String s1, String s2) {
        int res = (s1.length() > s2.length() ? 1 :
                (s1.length() < s2.length() ? -1 : 0));
        if (res == 0) {
            res = s1.compareTo(s2);
        }
        return res;
    }

    private String parseStr(byte[] array_byte, int encoding) throws UnsupportedEncodingException {
        String res;
        String encodingstr;
        switch (encoding) {
            case 33:
            case 223:
            case 83:
            case 254:
            case 45:
            case 46:
                encodingstr = "UTF-8";
                break;
            case 97:
            case 98:
                encodingstr = "euc_jp";
                break;
            case 24:
            case 86:
                encodingstr = "gb2312";
                break;
            case 19:
            case 85:
                encodingstr = "euc_kr";
                break;
            case 4:
            case 80:
                encodingstr = "cp850";
                break;
            case 40:
            case 81:
                encodingstr = "cp852";
                break;
            case 36:
            case 68:
                encodingstr = "cp866";
                break;
            case 5:
            case 8:
            case 15:
            case 31:
            case 47:
            case 48:
            case 49:
            case 94:
                encodingstr = "latin1";
                break;
            case 2:
            case 9:
            case 21:
            case 27:
            case 77:
                encodingstr = "latin2";
            case 25:
            case 70:
                encodingstr = "greek";
                break;
            case 16:
            case 71:
                encodingstr = "hebrew";
                break;
            case 30:
            case 78:
                encodingstr = "latin5";
                break;
            case 7:
            case 74:
                encodingstr = "koi8_r";
                break;
            case 22:
            case 75:
                encodingstr = "koi8_u";
                break;
            case 13:
            case 88:
                encodingstr = "sjis";
                break;
            case 18:
            case 89:
                encodingstr = "tis620";
                break;
            case 11:
            case 65:
                encodingstr = "ASCII";
                break;
            case 54:
            case 55:
                encodingstr = "utf16";
                break;
            case 35:
            case 159:
            case 90:
                encodingstr = "ISO-10646-UCS-2";
                break;
            case 56:
            case 62:
                encodingstr = "UTF_16LE";
                break;
            case 60:
            case 61:
                encodingstr = "UTF32";
                break;
            case 26:
            case 34:
            case 44:
            case 66:
            case 99:
                encodingstr = "cp1250";
                break;
            case 14:
            case 23:
            case 50:
            case 51:
            case 52:
                encodingstr = "cp1251";
                break;
            case 57:
            case 67:
                encodingstr = "cp1256";
                break;
            case 29:
            case 58:
            case 59:
                encodingstr = "cp1257";
                break;
            case 39:
            case 53:
                encodingstr = "MacRoman";
                break;
            default:
                throw new UnsupportedEncodingException();
        }
        res = new String(array_byte, encodingstr);
        return res;
    }

    private int put_sint(byte[] Blob, long vall, int offset) {
        vall = (vall << 1) ^ (vall < 0 ? (-1) : 0);
        int index = 0;
        while (vall != 0) {
            Blob[offset + index] = (byte) (vall & 0xFF);
            vall >>>= 8;
            index++;
        }
        return index;
    }

    private int put_uint(byte[] Blob, long vall, int offset) {
        int index = 0;
        while (vall != 0) {
            Blob[offset + index] = (byte) (vall & 0xFF);
            vall >>>= 8;
            index++;
        }
        return index;
    }

    private int put_double(byte[] Blob, double vall, int offset) {
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.putDouble(vall);
        bb.rewind();
        bb.get(Blob, offset, 8);
        return 8;
    }

    private int put_str(byte[] Blob, String vals, int offset) {
        int vall = 45;//UTF-8
        int index = 0;
        while (vall != 0) {
            Blob[offset + index] = (byte) (vall & 0xFF);
            vall >>>= 8;
            index++;
        }
        byte[] bytes = vals.getBytes();
        for (int i = 0; i < bytes.length; i++) {
            Blob[offset + index + i] = bytes[i];
        }
        return (index + bytes.length);
    }

    private int put_DynCol(byte[] Blob, byte[] vall, int offset) {
        for (int index = 0; index < vall.length; index++) {
            Blob[offset + index] = vall[index];
        }
        return vall.length;
    }

    private void save_Str(byte[] blob, Record rc, int real_offset_value, int byte_long) throws UnsupportedEncodingException {
        int encoding = blob[blb.header_offset + blb.nmpool_size + real_offset_value] & 0xFF;
        byte[] array_byte = new byte[byte_long];
        for (int index = 1; index < byte_long + 1; index++) {
            array_byte[index - 1] = blob[blb.header_offset + blb.nmpool_size + real_offset_value + index];
        }
        rc.str_value = parseStr(array_byte, encoding);
    }

    private void save_double(byte[] blob, Record rc, int real_offset_value, int byte_long) {
        byte[] array_byte = new byte[byte_long];
        for (int index = 0; index < byte_long; index++) {
            array_byte[index] = blob[blb.header_offset + blb.nmpool_size + real_offset_value + index];
        }
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(ByteOrder.LITTLE_ENDIAN);
        bb.put(array_byte);
        bb.rewind();
        rc.double_value = bb.getDouble();
    }

    private void save_uint(byte[] blob, Record rc, int real_offset_value, int byte_long) {
        long val = 0;
        for (int index = 0; index < byte_long; index++) {
            val = val | ((blob[blb.header_offset + blb.nmpool_size + real_offset_value + index] & 0xFF) << (8 * index));
        }
        if (val < 0) {
            throw new IllegalArgumentException();
        }
        rc.long_value = val;
    }

    private void save_sint(byte[] blob, Record rc, int real_offset_value, int byte_long) {
        long val = 0;
        for (int index = 0; index < byte_long; index++) {
            val = val | ((long) (blob[blb.header_offset + blb.nmpool_size + real_offset_value + index] & 0xFF) << (8 * index));
        }
        if ((val & 1) != 0)
            val = (val >> 1) ^ (-1);
        else
            val >>>= 1;
        rc.long_value = val;
    }

    private void save_DynCol(byte[] blob, Record rc, int real_offset_value, int byte_long) throws Exception {
        byte[] array_byte = new byte[byte_long];
        for (int index = 0; index < byte_long; index++) {
            array_byte[index] = blob[blb.header_offset + blb.nmpool_size + real_offset_value + index];
        }
        rc.DynCol_value = new DynCol();
        rc.DynCol_value.setBlob(array_byte);
    }

    public static void main(String[] args) throws Exception {
        //Numeric block
        DynCol d = new DynCol();
        d.setInt("1", 0);
        System.out.print("d.setInt(\"1\", 0): ");
        equel(d.getBlob(), "000100010000");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getInt("1") == 0);
        d.setString("1", null);
        System.out.print("d.setString(\"1\", null): ");
        equel(d.getBlob(), "000000");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getString("1") == null);
        d.setString("1", "afaf");
        System.out.print("d.setString(\"1\", \"afaf\"): ");
        equel(d.getBlob(), "0001000100032D61666166");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getString("1").equals("afaf"));
        d.setString("1", "1212");
        System.out.print("d.setString(\"1\", \"1212\"): ");
        equel(d.getBlob(), "0001000100032D31323132");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getString("1").equals("1212"));
        d.setString("1", "12.12");
        System.out.print("d.setString(\"1\", \"12.12\"): ");
        equel(d.getBlob(), "0001000100032D31322E3132");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getString("1").equals("12.12"));
        d.setString("1", "99999999999999999999999999999");
        System.out.print("d.setString(\"1\", \"99999999999999999999999999999\"): ");
        equel(d.getBlob(), "0001000100032D3939393939393939393939393939393939393939393939393939393939");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"1\"): ");
        equel(d.getString("1").equals("99999999999999999999999999999"));
        d.remove("1");
        System.out.print("d.remove(\"1\"): ");
        equel(d.getBlob(), "000000");
        d.setUint("1", 1212);
        System.out.print("d.setUint(\"1\", 1212): ");
        equel(d.getBlob(), "000100010001BC04");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 1212);
        d.setUint("1", 7);
        System.out.print("d.setUint(\"1\", 7): ");
        equel(d.getBlob(), "00010001000107");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 7);
        d.setUint("1", 8);
        System.out.print("d.setUint(\"1\", 8): ");
        equel(d.getBlob(), "00010001000108");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 8);
        d.setUint("1", 127);
        System.out.print("d.setUint(\"1\", 127): ");
        equel(d.getBlob(), "0001000100017F");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 127);
        d.setUint("1", 128);
        System.out.print("d.setUint(\"1\", 128): ");
        equel(d.getBlob(), "00010001000180");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 128);
        d.setInt("1", -1);
        System.out.print("d.setInt(\"1\", -1): ");
        equel(d.getBlob(), "00010001000001");
        d.setBlob(d.getBlob());
        System.out.print("d.getInt(\"1\"): ");
        equel(d.getInt("1") == -1);
        d.setInt("1", Long.MAX_VALUE);
        System.out.print("d.setInt(\"1\", Long.MAX_VALUE): ");
        equel(d.getBlob(), "000100010000FEFFFFFFFFFFFFFF");
        d.setBlob(d.getBlob());
        System.out.print("d.getInt(\"1\"): ");
        equel(d.getInt("1") == Long.MAX_VALUE);
        d.setDouble("1", 1212);
        System.out.print("d.setDouble(\"1\", 1212): ");
        equel(d.getBlob(), "0001000100020000000000F09240");
        d.setBlob(d.getBlob());
        System.out.print("d.getDouble(\"1\"): ");
        equel(d.getDouble("1") == 1212);
        d.setDouble("1", 12.12);
        System.out.print("d.setDouble(\"1\", 12.12): ");
        equel(d.getBlob(), "0001000100023D0AD7A3703D2840");
        d.setBlob(d.getBlob());
        System.out.print("d.getDouble(\"1\"): ");
        equel(d.getDouble("1") == 12.12);
        d.setJson("{\"1\":1," +
                "\"2\":-1," +
                "\"3\":12.12," +
                "\"4\":\"afaf\"}");
        System.out.print("d.setJson(\"{\"1\":1, \"2\":-1,\"3\":12.12,\"4\":\"afaf\"}\"): ");
        equel(d.getBlob(), "00040001000102000803001204005301013D0AD7A3703D28402D61666166");
        d.setBlob(d.getBlob());
        System.out.print("d.getJson(): ");
        System.out.println(d.getJson());
        System.out.print("d.getUint(\"1\"): ");
        equel(d.getUint("1") == 1);
        System.out.print("d.getInt(\"2\"): ");
        equel(d.getInt("2") == -1);
        System.out.print("d.getDouble(\"3\"): ");
        equel(d.getDouble("3") == 12.12);
        System.out.print("d.getString(\"4\"): ");
        equel(d.getString("4").equals("afaf"));
        //Named block
        d = new DynCol();
        d.setString("n", null);
        System.out.print("d.setString(\"n\", null): ");
        equel(d.getBlob(), "000000");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"n\"): ");
        equel(d.getString("n") == null);
        d.setString("n", "afaf");
        System.out.print("d.setString(\"n\", \"afaf\"): ");
        equel(d.getBlob(), "0401000100000003006E2D61666166");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"n\"): ");
        equel(d.getString("n").equals("afaf"));
        d.setString("n", "1212");
        System.out.print("d.setString(\"n\", \"1212\"): ");
        equel(d.getBlob(), "0401000100000003006E2D31323132");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"n\"): ");
        equel(d.getString("n").equals("1212"));
        d.setString("n", "12.12");
        System.out.print("d.setString(\"n\", \"12.12\"): ");
        equel(d.getBlob(), "0401000100000003006E2D31322E3132");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"n\"): ");
        equel(d.getString("n").equals("12.12"));
        d.setString("n", "99999999999999999999999999999");
        System.out.print("d.setString(\"n\", \"99999999999999999999999999999\"): ");
        equel(d.getBlob(), "0401000100000003006E2D3939393939393939393939393939393939393939393939393939393939");
        d.setBlob(d.getBlob());
        System.out.print("d.getString(\"n\"): ");
        equel(d.getString("n").equals("99999999999999999999999999999"));
        d.remove("n");
        System.out.print("d.remove(\"n\"): ");
        equel(d.getBlob(), "000000");
        d.setUint("n", 1212);
        System.out.print("d.setUint(\"n\", 1212): ");
        equel(d.getBlob(), "0401000100000001006EBC04");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 1212);
        d.setUint("n", 7);
        System.out.print("d.setUint(\"n\", 7): ");
        equel(d.getBlob(), "0401000100000001006E07");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 7);
        d.setUint("n", 8);
        System.out.print("d.setUint(\"n\", 8): ");
        equel(d.getBlob(), "0401000100000001006E08");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 8);
        d.setUint("n", 127);
        System.out.print("d.setUint(\"n\", 127): ");
        equel(d.getBlob(), "0401000100000001006E7F");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 127);
        d.setUint("n", 128);
        System.out.print("d.setUint(\"n\", 128): ");
        equel(d.getBlob(), "0401000100000001006E80");
        d.setBlob(d.getBlob());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 128);
        d.setInt("n", -1);
        System.out.print("d.setInt(\"n\", -1): ");
        equel(d.getBlob(), "0401000100000000006E01");
        d.setBlob(d.getBlob());
        System.out.print("d.getInt(\"n\"): ");
        equel(d.getInt("n") == -1);
        d.setInt("n", Long.MAX_VALUE);
        System.out.print("d.setInt(\"n\", Long.MAX_VALUE): ");
        equel(d.getBlob(), "0401000100000000006EFEFFFFFFFFFFFFFF");
        d.setBlob(d.getBlob());
        System.out.print("d.getInt(\"n\"): ");
        equel(d.getInt("n") == Long.MAX_VALUE);
        d.setDouble("n", 1212);
        System.out.print("d.setDouble(\"n\", 1212): ");
        equel(d.getBlob(), "0401000100000002006E0000000000F09240");
        d.setBlob(d.getBlob());
        System.out.print("d.getDouble(\"n\"): ");
        equel(d.getDouble("n") == 1212);
        d.setDouble("n", 12.12);
        System.out.print("d.setDouble(\"n\", 12.12): ");
        equel(d.getBlob(), "0401000100000002006E3D0AD7A3703D2840");
        d.setBlob(d.getBlob());
        System.out.print("d.getDouble(\"n\"): ");
        equel(d.getDouble("n") == 12.12);
        d.setJson("{\"n\":1," +
                "\"2\":-1," +
                "\"3\":12.12," +
                "\"4\":\"afaf\"}");
        System.out.print("d.setJson(\"{\"n\":1, \"2\":-1,\"3\":12.12,\"4\":\"afaf\"}\"): ");
        equel(d.getBlob(), "04040004000000000001001200020093000300E1003233346E013D0AD7A3703D28402D6166616601");
        d.setBlob(d.getBlob());
        System.out.print("d.getJson(): ");
        System.out.println(d.getJson());
        System.out.print("d.getUint(\"n\"): ");
        equel(d.getUint("n") == 1);
        System.out.print("d.getInt(\"2\"): ");
        equel(d.getInt("2") == -1);
        System.out.print("d.getDouble(\"3\"): ");
        equel(d.getDouble("3") == 12.12);
        System.out.print("d.getString(\"4\"): ");
        equel(d.getString("4").equals("afaf"));
        d.setDynCol("n", d);
        System.out.print("d.setDynCol(\"n\", d): ");
        equel(d.getBlob(), "04040004000000000001001200020093000300E8003233346E013D0AD7A3703D28402D6166616604040004000000000001001200020093000300E1003233346E013D0AD7A3703D28402D6166616601");
    }

    private static void equel(byte[] blb, String str) {
        if (str.equals(javax.xml.bind.DatatypeConverter.printHexBinary(blb))) {
            System.out.println("success");
        } else {
            System.out.print("mistake: ");
            System.out.println(javax.xml.bind.DatatypeConverter.printHexBinary(blb));
        }
    }

    private static void equel(boolean val) {
        if (val) {
            System.out.println("success");
        } else {
            System.out.println("mistake");
        }
    }
}