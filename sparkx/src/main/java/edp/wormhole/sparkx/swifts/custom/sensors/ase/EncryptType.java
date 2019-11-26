package edp.wormhole.sparkx.swifts.custom.sensors.ase;

import java.math.BigInteger;

import org.apache.commons.codec.binary.Base64;
/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/20 14:51
 * To change this template use File | Settings | File Templates.
 */
/**
 * AES加密解密方式
 */
public enum EncryptType {

    OLD_PARAMETER, API_INTERNAL, P2P_SERVICE, ;

    /**
     * App加密解密使用的key
     */
    @Deprecated
    private static String LEGACY_SECURITY_AES_KEY = "0C5E75A210884F61";

    /**
     * yingApi内部加密解密使用的key
     */
    private static String API_INTERAL_AES_KEY = "5FF2712BF8444D13A01244904D611F8E";

    /**
     * p2pService加密解密使用的key
     */
    private static String YRD_PARAM_KEY = "JCQ15PykRU2j/Diqe2uPtQ==";

    static {
        OLD_PARAMETER.aesKey = LEGACY_SECURITY_AES_KEY;
        OLD_PARAMETER.keyLength = 16;
        OLD_PARAMETER.mode = BackAES.TRANSFORMATION_TYPE_ECB;

        API_INTERNAL.aesKey = API_INTERAL_AES_KEY;
        API_INTERNAL.keyLength = 32;
        API_INTERNAL.mode = BackAES.TRANSFORMATION_TYPE_ECB;

        P2P_SERVICE.aesKey = YRD_PARAM_KEY;
        P2P_SERVICE.keyLength = 16;
        P2P_SERVICE.mode = BackAES.TRANSFORMATION_TYPE_ECB;
    }

    private String aesKey;

    @SuppressWarnings("unused")
    private int keyLength;

    private int mode;

    public byte[] getKey() {
        if (this.aesKey.equals(API_INTERAL_AES_KEY)) {
            return new BigInteger(this.aesKey, 16).toByteArray();
        } else if (this.aesKey.equals(YRD_PARAM_KEY)) {
            return Base64.decodeBase64(this.aesKey);
        }
        return this.aesKey.getBytes();
    }

    public int getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return "EncryptType{" + "aesKey='" + aesKey + '\'' + ", mode=" + mode
                + '}';
    }
}
