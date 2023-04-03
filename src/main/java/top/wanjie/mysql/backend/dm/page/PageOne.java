package top.wanjie.mysql.backend.dm.page;

import top.wanjie.mysql.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/01/15:58
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;
    private static final int PAGE_SIZE = 1 << 13;

    public static byte[] initRaw() {
        byte[] raw = new byte[PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC),0,raw,OF_VC,LEN_VC);
    }

    public static void setVcOpen(Page page) {
        setVcOpen(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw,OF_VC,raw,OF_VC + LEN_VC,LEN_VC);
    }

    public static void setVcClose(Page page) {
        setVcClose(page.getData());
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw,OF_VC,LEN_VC),Arrays.copyOfRange(raw,OF_VC + LEN_VC,OF_VC + 2 * LEN_VC));
    }
}
