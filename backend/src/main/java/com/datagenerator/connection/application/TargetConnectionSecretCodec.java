package com.datagenerator.connection.application;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TargetConnectionSecretCodec {

    private static final String PREFIX = "enc:v1:";
    private static final String CIPHER = "AES/GCM/NoPadding";
    private static final int GCM_TAG_BITS = 128;
    private static final int IV_BYTES = 12;

    private final SecureRandom secureRandom = new SecureRandom();
    private final SecretKeySpec secretKey;

    public TargetConnectionSecretCodec(@Value("${mdg.secret.key:local-dev-secret-change-me}") String secretKeyText) {
        this.secretKey = new SecretKeySpec(deriveKey(secretKeyText), "AES");
    }

    public String encryptForStorage(String value) {
        if (value == null || value.isBlank() || isEncrypted(value)) {
            return value == null ? "" : value;
        }
        try {
            byte[] iv = new byte[IV_BYTES];
            secureRandom.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] encrypted = cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));
            ByteBuffer payload = ByteBuffer.allocate(iv.length + encrypted.length);
            payload.put(iv);
            payload.put(encrypted);
            return PREFIX + Base64.getEncoder().encodeToString(payload.array());
        } catch (Exception exception) {
            throw new IllegalStateException("连接密码加密失败: " + exception.getMessage(), exception);
        }
    }

    public String decryptForUse(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        if (!isEncrypted(value)) {
            return value;
        }
        try {
            byte[] payload = Base64.getDecoder().decode(value.substring(PREFIX.length()));
            if (payload.length <= IV_BYTES) {
                throw new IllegalArgumentException("密文长度不正确");
            }
            byte[] iv = new byte[IV_BYTES];
            byte[] encrypted = new byte[payload.length - IV_BYTES];
            System.arraycopy(payload, 0, iv, 0, IV_BYTES);
            System.arraycopy(payload, IV_BYTES, encrypted, 0, encrypted.length);

            Cipher cipher = Cipher.getInstance(CIPHER);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, new GCMParameterSpec(GCM_TAG_BITS, iv));
            return new String(cipher.doFinal(encrypted), StandardCharsets.UTF_8);
        } catch (Exception exception) {
            throw new IllegalStateException("连接密码解密失败，请确认 MDG_SECRET_KEY 与加密时一致", exception);
        }
    }

    public boolean isEncrypted(String value) {
        return value != null && value.startsWith(PREFIX);
    }

    private byte[] deriveKey(String secretKeyText) {
        try {
            String normalized = secretKeyText == null || secretKeyText.isBlank()
                    ? "local-dev-secret-change-me"
                    : secretKeyText;
            return MessageDigest.getInstance("SHA-256").digest(normalized.getBytes(StandardCharsets.UTF_8));
        } catch (Exception exception) {
            throw new IllegalStateException("连接密码密钥初始化失败", exception);
        }
    }
}
