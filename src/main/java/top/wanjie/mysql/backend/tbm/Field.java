package top.wanjie.mysql.backend.tbm;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/08/21:03
 */

import com.google.common.primitives.Bytes;
import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.im.BPlusTree;
import top.wanjie.mysql.backend.parser.statement.SingleExpression;
import top.wanjie.mysql.backend.tm.TransactionManagerImpl;
import top.wanjie.mysql.backend.utils.Panic;
import top.wanjie.mysql.backend.utils.ParseStringRes;
import top.wanjie.mysql.backend.utils.Parser;

import java.util.Arrays;
import java.util.List;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid]
 * 如果field无索引，IndexUid为0
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;

    private BPlusTree bptree;
    private long index;

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try{
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(tb, uid).parseSelf(raw);
    }

    public Field(Table tb, long uid) {
        this.tb = tb;
        this.uid = uid;
    }

    public Field(Table tb, String fieldName, String fieldType, long uid) {
        this.tb = tb;
        this.fieldType = fieldType;
        this.fieldName = fieldName;
        this.uid = uid;
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed) throws Exception{
        typeCheck(fieldType);
        Field f = new Field(tb, fieldName, fieldType, 0);
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bptree = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = Parser.string2Byte(fieldName);
        byte[] typeRaw = Parser.string2Byte(fieldType);
        byte[] indexRaw = Parser.long2Byte(index);
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw));
    }

    public Field parseSelf(byte[] raw){
        int position = 0;
        ParseStringRes parseStringRes = Parser.parseString(raw);
        fieldName = parseStringRes.str;
        position += parseStringRes.next;
        parseStringRes = Parser.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = parseStringRes.str;
        position += parseStringRes.next;
        index = Parser.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if(index != 0) {
            try {
                bptree = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch (Exception e) {
                Panic.panic(e);
            }
        }
        return this;
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }


    //uid为该entry所在磁盘位置
    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Key(key);
        bptree.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bptree.searchRange(left, right);
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public long value2Key(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = Parser.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = Parser.int2Byte((int)v);
                break;
            case "int64":
                raw = Parser.long2Byte((long)v);
                break;
            case "string":
                raw = Parser.string2Byte((String)v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    // 根据字段类型计算出字段值与字段偏移
    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = Parser.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = Parser.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = Parser.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(")")
                .toString();
    }

    class FieldCalRes {
        public long left;
        public long right;
    }

    public FieldCalRes calExp(SingleExpression exp) throws Exception{
        Object v = null;
        FieldCalRes res = new FieldCalRes();
        switch (exp.compareOp){
            case "<":
                res.left = 0;
                v = string2Value(exp.value);
                res.right = value2Key(v);
                if(res.right > 0) res.right--;
                break;
            case "=":
                v = string2Value(exp.value);
                res.left = res.right = value2Key(v);
                break;
            case ">":
                res.right = Long.MAX_VALUE;
                v = string2Value(exp.value);
                res.left = value2Key(v) + 1;
                break;
        }
        return res;
    }
}
