package top.wanjie.mysql.backend.dm.page;

import top.wanjie.mysql.backend.utils.Parser;

import java.util.Arrays;

/**
 * @Author fraven
 * @Description 普通页，存放数据库数据
 * @Date 2023/04/01/16:25
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageImpl.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageImpl.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw,short fso) {
        System.arraycopy(Parser.short2Byte(fso),0,raw,0,2);
    }

    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    public static short insert(Page pg,byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    public static int getFreeSpage(Page pg) {
        return PageImpl.PAGE_SIZE - getFSO(pg);
    }

    public static void recoverInsert(Page pg, short offset, byte[] raw) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short fso = getFSO(pg);
        if(fso < offset + raw.length) {
            setFSO(pg.getData(), (short) (raw.length + offset));
        }
    }

    public static void recoverUpdate(Page pg, short offset, byte[] raw) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

    }





}
