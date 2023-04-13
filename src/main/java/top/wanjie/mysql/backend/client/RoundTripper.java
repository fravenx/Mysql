package top.wanjie.mysql.backend.client;


import top.wanjie.mysql.backend.transport.Packager;
import top.wanjie.mysql.backend.transport.Package;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
