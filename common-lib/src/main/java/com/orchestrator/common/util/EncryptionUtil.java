package com.orchestrator.common.util;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM encryption utility for sensitive task payloads.
 * 
 * Format: base64(IV[12] + ciphertext + authTag[16])
 * The 12-byte IV is randomly generated and prepended to the ciphertext.
 */
public final class EncryptionUtil {

    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128; // bits

    private EncryptionUtil() {}

    /**
     * Encrypts plaintext using AES-256-GCM.
     *
     * @param plaintext the data to encrypt
     * @param base64Key base64-encoded 256-bit AES key
     * @return base64-encoded ciphertext (IV prepended)
     */
    public static String encrypt(String plaintext, String base64Key) {
        try {
            byte[] key = Base64.getDecoder().decode(base64Key);
            SecretKeySpec keySpec = new SecretKeySpec(key, "AES");

            byte[] iv = new byte[GCM_IV_LENGTH];
            new SecureRandom().nextBytes(iv);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] ciphertext = cipher.doFinal(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));

            // Prepend IV to ciphertext
            byte[] combined = new byte[iv.length + ciphertext.length];
            System.arraycopy(iv, 0, combined, 0, iv.length);
            System.arraycopy(ciphertext, 0, combined, iv.length, ciphertext.length);

            return Base64.getEncoder().encodeToString(combined);
        } catch (Exception ex) {
            throw new RuntimeException("Encryption failed", ex);
        }
    }

    /**
     * Decrypts AES-256-GCM ciphertext.
     *
     * @param encryptedBase64 base64-encoded ciphertext (IV prepended)
     * @param base64Key base64-encoded 256-bit AES key
     * @return decrypted plaintext
     */
    public static String decrypt(String encryptedBase64, String base64Key) {
        try {
            byte[] combined = Base64.getDecoder().decode(encryptedBase64);
            byte[] key = Base64.getDecoder().decode(base64Key);
            SecretKeySpec keySpec = new SecretKeySpec(key, "AES");

            byte[] iv = new byte[GCM_IV_LENGTH];
            System.arraycopy(combined, 0, iv, 0, GCM_IV_LENGTH);

            byte[] ciphertext = new byte[combined.length - GCM_IV_LENGTH];
            System.arraycopy(combined, GCM_IV_LENGTH, ciphertext, 0, ciphertext.length);

            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, keySpec, new GCMParameterSpec(GCM_TAG_LENGTH, iv));

            byte[] plaintext = cipher.doFinal(ciphertext);
            return new String(plaintext, java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception ex) {
            throw new RuntimeException("Decryption failed", ex);
        }
    }
}
