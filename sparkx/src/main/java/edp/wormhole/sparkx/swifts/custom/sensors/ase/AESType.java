package edp.wormhole.sparkx.swifts.custom.sensors.ase;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/20 14:46
 * To change this template use File | Settings | File Templates.
 */
public enum AESType {

    ECB("ECB", "0"), CBC("CBC", "1"), CFB("CFB", "2"), OFB("OFB", "3");

    private String k;
    private String v;

    private AESType(String k, String v) {
        this.k = k;
        this.v = v;
    }

    public String key() {
        return this.k;
    }

    public String value() {
        return this.v;
    }

    public static AESType get(int id) {
        AESType[] vs = AESType.values();
        for (int i = 0; i < vs.length; i++) {
            AESType d = vs[i];
            if (String.valueOf(id).equals(d.key()))
                return d;
        }
        return AESType.CBC;
    }

}
