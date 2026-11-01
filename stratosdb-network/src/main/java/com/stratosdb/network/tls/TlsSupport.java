package com.stratosdb.network.tls;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * TLS setup for the server socket and client connections.
 *
 * Supports:
 * - TLS 1.3 and 1.2
 * - Configurable Keystore (JKS and PKCS12)
 * - Configurable Truststore for client certificate authentication
 * - Keytool certificate generation helpers for automated integration testing
 */
public final class TlsSupport {
    private TlsSupport() {}

    /** Loads a server SSLContext from a keystore file containing a certificate and its private key. */
    public static SSLContext loadServerContext(String keystorePath, char[] keystorePassword)
            throws GeneralSecurityException, IOException {
        return loadServerContext(keystorePath, keystorePassword, null, null, null, null, null);
    }

    public static SSLContext loadServerContext(String keystorePath, char[] keystorePassword,
                                             String truststorePath, char[] truststorePassword)
            throws GeneralSecurityException, IOException {
        return loadServerContext(keystorePath, keystorePassword, null, null, truststorePath, truststorePassword, null);
    }

    public static SSLContext loadServerContext(String keystorePath, char[] keystorePassword, String keystoreType, char[] keyPassword,
                                             String truststorePath, char[] truststorePassword, String truststoreType)
            throws GeneralSecurityException, IOException {
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword, keystoreType);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword != null ? keyPassword : keystorePassword);

        TrustManager[] trustManagers = null;
        if (truststorePath != null && !truststorePath.isBlank()) {
            KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword, truststoreType);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            trustManagers = tmf.getTrustManagers();
        }

        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(kmf.getKeyManagers(), trustManagers, new SecureRandom());
        return context;
    }

    /**
     * Loads a client SSLContext verifying the server certificate against a truststore,
     * and optionally providing a client certificate from a client keystore.
     */
    public static SSLContext loadClientContext(String truststorePath, char[] truststorePassword,
                                             String clientKeystorePath, char[] clientKeystorePassword)
            throws GeneralSecurityException, IOException {
        TrustManager[] trustManagers = null;
        if (truststorePath != null && !truststorePath.isBlank()) {
            KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword, null);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);
            trustManagers = tmf.getTrustManagers();
        }

        KeyManager[] keyManagers = null;
        if (clientKeystorePath != null && !clientKeystorePath.isBlank()) {
            KeyStore keyStore = loadKeyStore(clientKeystorePath, clientKeystorePassword, null);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, clientKeystorePassword);
            keyManagers = kmf.getKeyManagers();
        }

        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keyManagers, trustManagers, new SecureRandom());
        return context;
    }

    /**
     * For local development and testing ONLY: trusts any server certificate
     * with zero verification. Never use this against a network you don't fully control.
     */
    public static SSLContext insecureTrustAllClientContext() throws GeneralSecurityException {
        return insecureTrustAllClientContext(null, null);
    }

    public static SSLContext insecureTrustAllClientContext(String clientKeystorePath, char[] clientKeystorePassword)
            throws GeneralSecurityException {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }};

        KeyManager[] keyManagers = null;
        if (clientKeystorePath != null && !clientKeystorePath.isBlank()) {
            try {
                KeyStore keyStore = loadKeyStore(clientKeystorePath, clientKeystorePassword, null);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(keyStore, clientKeystorePassword);
                keyManagers = kmf.getKeyManagers();
            } catch (IOException e) {
                throw new GeneralSecurityException("Failed to load client keystore", e);
            }
        }

        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(keyManagers, trustAllCerts, new SecureRandom());
        return context;
    }

    public static String extractCommonName(X509Certificate cert) {
        if (cert == null) return null;
        String dn = cert.getSubjectX500Principal().getName();
        for (String part : dn.split(",")) {
            part = part.trim();
            if (part.toUpperCase().startsWith("CN=")) {
                return part.substring(3).trim();
            }
        }
        return null;
    }

    public static KeyStore loadKeyStore(String path, char[] password, String explicitType)
            throws GeneralSecurityException, IOException {
        String type = explicitType;
        if (type == null || type.isBlank()) {
            type = (path.endsWith(".p12") || path.endsWith(".pfx")) ? "PKCS12" : "JKS";
        }
        KeyStore keyStore = KeyStore.getInstance(type);
        try (FileInputStream in = new FileInputStream(path)) {
            keyStore.load(in, password);
        }
        return keyStore;
    }

    /**
     * Generates a self-signed keypair and certificate in PKCS12 format using the JDK's keytool.
     */
    public static void generateKeyStore(String path, char[] password, String dname, String san) throws Exception {
        String keytool = System.getProperty("java.home") + "/bin/keytool";
        List<String> cmd = new ArrayList<>(List.of(
            keytool, "-genkeypair",
            "-alias", "stratos-tls",
            "-keyalg", "RSA", "-keysize", "2048", "-validity", "365",
            "-keystore", path, "-storetype", "PKCS12",
            "-storepass", new String(password), "-keypass", new String(password),
            "-dname", dname
        ));
        if (san != null && !san.isBlank()) {
            cmd.add("-ext");
            cmd.add("SAN=" + san);
        }
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("keytool failed to generate keystore:\n" + output);
        }
    }

    /**
     * Exports a certificate in RFC/PEM format from a keystore using keytool.
     */
    public static void exportCertificate(String keystorePath, char[] password, String certPath) throws Exception {
        String keytool = System.getProperty("java.home") + "/bin/keytool";
        ProcessBuilder pb = new ProcessBuilder(
            keytool, "-exportcert",
            "-alias", "stratos-tls",
            "-keystore", keystorePath,
            "-storepass", new String(password),
            "-rfc", "-file", certPath
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("keytool exportcert failed:\n" + output);
        }
    }

    /**
     * Imports a certificate into a truststore using keytool.
     */
    public static void importCertificate(String truststorePath, char[] password, String alias, String certPath) throws Exception {
        String keytool = System.getProperty("java.home") + "/bin/keytool";
        ProcessBuilder pb = new ProcessBuilder(
            keytool, "-importcert",
            "-alias", alias,
            "-file", certPath,
            "-keystore", truststorePath,
            "-storetype", "PKCS12",
            "-storepass", new String(password),
            "-noprompt"
        );
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IllegalStateException("keytool importcert failed:\n" + output);
        }
    }
}
