package edp.wormhole.sparkx.swifts.custom.sensors.ase;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/20 14:49
 * To change this template use File | Settings | File Templates.
 */
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class AESUtil {

    private static String SECURITY_AESKEY = "0C5E75A210884F61";
    /**
     * p2pService加密解密使用的key
     */
    private static String YRD_PARAM_KEY = "JCQ15PykRU2j/Diqe2uPtQ==";

    /**
     * (旧)yingApi和App间敏感字段的加密，包括如下： yingApi -> App: 借款人信息接口返回值中的敏感字段
     */
    public static String encrypt(String content) {
        try {
            return new String(BackAES.encrypt(content, SECURITY_AESKEY, 0));
        } catch (Exception e) {
            System.out.println("字段加密失败, content=" + content + e);
        }
        return null;
    }

    /**
     * (旧)yingApi和App间敏感字段的解密，包括如下： App -> yingApi: 各种接口调用参数中的敏感字段，例如银行卡号、手机号
     */
    public static String decrypt(String content) {
        String decryptString = null;
        try {
            decryptString = BackAES.decrypt(content, SECURITY_AESKEY, 0);
        } catch (Exception e) {
            System.out.println("字段解密失败, content=" + content + e);
        }
        return decryptString;
    }

    /**
     * 不同场景的加密
     *
     * @param content
     * @param encryptType
     * @return
     */
    public static String encrypt(String content, EncryptType encryptType) {
        try {
            return BackAES.encrypt(content, encryptType);
        } catch (Exception e) {
            System.out.println("字段加密失败, content=" + content + ",encryptType="
                    + encryptType + e);
            return null;
        }
    }

    /**
     * 不同场景的解密
     *
     * @param content
     * @param encryptType
     * @return
     */
    public static String decrypt(String content, EncryptType encryptType) {
        try {
            return BackAES.decrypt(content, encryptType);
        } catch (Exception e) {
            System.out.println("字段解密失败, content=" + content + ",encryptType="
                    + encryptType + e);
            return null;
        }
    }

    /**
     * p2pservice敏感字段解密
     *
     * @param data
     * @return
     * @throws Exception
     */
    public static String decryptForYrd(String data) throws Exception {
        Cipher cipher = Cipher.getInstance(BackAES.selectMod(0));

        cipher.init(Cipher.DECRYPT_MODE,
                new SecretKeySpec(Base64.decodeBase64(YRD_PARAM_KEY), "AES"));
        return new String(cipher.doFinal(Base64.decodeBase64(data)));
    }

    /**
     * 调用p2pservice时敏感字段进行加密
     *
     * @param data
     * @return
     * @throws GeneralSecurityException
     */
    public static String encryptForYrd(String data)
            throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(BackAES.selectMod(0));
        cipher.init(Cipher.ENCRYPT_MODE,
                new SecretKeySpec(Base64.decodeBase64(YRD_PARAM_KEY), "AES"));
        return Base64.encodeBase64String(cipher.doFinal(data.getBytes()));
    }

    public static void main(String[] args) {
        String str="4qajaRRBEgH4q0O4n7GhTBEKsWPAM2AKFrVofWpI8TfQmZGCpQNeKEvPn7WPLmvT";
        System.out.println(decrypt("90521e3203711fbb"));
    }

}
