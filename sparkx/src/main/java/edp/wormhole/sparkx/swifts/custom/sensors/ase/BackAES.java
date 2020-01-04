package edp.wormhole.sparkx.swifts.custom.sensors.ase;

import java.io.IOException;
import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
/**
 * Created by IntelliJ IDEA.
 *
 * @Author daemon
 * @Date 19/11/20 14:50
 * To change this template use File | Settings | File Templates.
 */
public class BackAES {
    /** 默认偏移 */
    private static String ivParameter = "1234567890123456";//

    private static String TRANSFORMATION_MODE = "";
    private static boolean isPwd = false;

    private static int pwdLength = 16;
    private static String val = "0";

    /** ECB */
    public static final int TRANSFORMATION_TYPE_ECB = 0;
    /** CBC */
    public static final int TRANSFORMATION_TYPE_CBC = 1;
    /** CFB */
    public static final int TRANSFORMATION_TYPE_CFB = 2;
    /** OFB */
    public static final int TRANSFORMATION_TYPE_OFB = 3;
    /** AES */
    public static final String AES_WAYS = "AES";
    /** UTF_8 */
    public static final String UTF_8 = "utf-8";
    /** PKCS5Padding */
    public static final String PADDING_PKCS5PADDING = "PKCS5Padding";
    /** 斜杠 */
    public static final String SLASH = "/";

    public static String selectMod(int type) {
        switch (type) {
            case TRANSFORMATION_TYPE_ECB:
                isPwd = false;
                TRANSFORMATION_MODE = AES_WAYS + SLASH + AESType.ECB.key() + SLASH
                        + PADDING_PKCS5PADDING;
                break;
            case TRANSFORMATION_TYPE_CBC:
                isPwd = true;
                TRANSFORMATION_MODE = AES_WAYS + SLASH + AESType.CBC.key() + SLASH
                        + PADDING_PKCS5PADDING;
                break;
            case TRANSFORMATION_TYPE_CFB:
                isPwd = true;
                TRANSFORMATION_MODE = AES_WAYS + SLASH + AESType.CFB.key() + SLASH
                        + PADDING_PKCS5PADDING;
                break;
            case TRANSFORMATION_TYPE_OFB:
                isPwd = true;
                TRANSFORMATION_MODE = AES_WAYS + SLASH + AESType.OFB.key() + SLASH
                        + PADDING_PKCS5PADDING;
                break;
        }
        return TRANSFORMATION_MODE;

    }

    /**
     * 加密方式一（密匙必须为16位）
     *
     * @param encryptedSrc
     * @param encryptKey
     * @param cipherType
     * @return
     * @throws Exception
     */
    public static byte[] encrypt(String encryptedSrc, String encryptKey,
                                 int cipherType) throws GeneralSecurityException, IOException {
        encryptKey = toMakeKey(encryptKey, pwdLength, val);
        Cipher cipher = Cipher.getInstance(selectMod(cipherType));
        byte[] raw = encryptKey.getBytes();
        SecretKeySpec skeySpec = new SecretKeySpec(raw, AES_WAYS);
        // 使用CBC模式，需要一个向量iv，可增加加密算法的强度
        IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
        if (!isPwd) {
            // ECB 不用密码
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec);
        } else {
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
        }
        byte[] encrypted = cipher.doFinal(encryptedSrc.getBytes(UTF_8));
        // 使用BASE64做转码。
        byte[] encryptedBase64 = Base64.encodeBase64(encrypted);
        return encryptedBase64;
    }

    /**
     * 解密方式一
     *
     * @param decryptedSrc
     * @param decryptKey
     * @param cipherType
     * @return
     * @throws Exception
     */
    public static String decrypt(String decryptedSrc, String decryptKey,
                                 int cipherType) throws GeneralSecurityException, IOException {
        String originalString = null;
        String decryptKeyMade = toMakeKey(decryptKey, pwdLength, val);
        try {
            byte[] raw = decryptKeyMade.getBytes("ASCII");
            SecretKeySpec skeySpec = new SecretKeySpec(raw, AES_WAYS);
            Cipher cipher = Cipher.getInstance(selectMod(cipherType));
            IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
            if (!isPwd) {
                // ECB 不用密码
                cipher.init(Cipher.DECRYPT_MODE, skeySpec);
            } else {
                cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            }
            // 先用base64解密
            byte[] encrypted1 = Base64.decodeBase64(decryptedSrc.getBytes());
            byte[] original = cipher.doFinal(encrypted1);
            originalString = new String(original, UTF_8);
        } catch (Exception ex) {
            System.out.println(ex);
            return null;
        }
        return originalString;
    }

    /**
     * 处理key，长度要不小于设定的长度(16/32位)，不足的在后面补值
     *
     * @param str
     * @param keyLength
     * @param val
     * @return
     */
    private static String toMakeKey(String str, int keyLength, String val) {
        int strLen = str.length();
        if (strLen < keyLength) {
            while (strLen < keyLength) {
                StringBuffer buffer = new StringBuffer();
                buffer.append(str).append(val);
                str = buffer.toString();
                strLen = str.length();
            }
        }
        return str;
    }

    public static String encrypt(String src, EncryptType encryptType)
            throws GeneralSecurityException, IOException {
        byte[] raw = encryptType.getKey();
        SecretKeySpec keySpec = new SecretKeySpec(raw, AES_WAYS);
        Cipher cipher = Cipher.getInstance(selectMod(encryptType.getMode()));
        if (isPwd) {
            // 向量iv，可增加加密算法的强度
            IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, iv);
        } else {
            // ECB 不用密码
            cipher.init(Cipher.ENCRYPT_MODE, keySpec);
        }
        // src -> utf-8 -> encrypt -> base64
        byte[] encrypted = cipher.doFinal(src.getBytes(UTF_8));
        return new String(Base64.encodeBase64(encrypted));
    }

    public static String decrypt(String src, EncryptType encryptType)
            throws GeneralSecurityException, IOException {
        byte[] raw = encryptType.getKey();
        SecretKeySpec keySpec = new SecretKeySpec(raw, AES_WAYS);
        Cipher cipher = Cipher.getInstance(selectMod(encryptType.getMode()));
        if (isPwd) {
            IvParameterSpec iv = new IvParameterSpec(ivParameter.getBytes());
            cipher.init(Cipher.DECRYPT_MODE, keySpec, iv);
        } else {
            // ECB不用密码
            cipher.init(Cipher.DECRYPT_MODE, keySpec);
        }
        // src -> base64(reverse) -> decrypt -> utf-8
        byte[] original = cipher.doFinal(Base64.decodeBase64(src.getBytes()));
        return new String(original, UTF_8);
    }

}
