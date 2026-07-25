package com.stratosdb.network.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

/**
 * TLS setup for the server socket (real certificate verification) and for
 * JDBC clients (currently trust-all only - a real, named gap, not hidden).
 *
 * Server side is real: loads an actual certificate + private key from a
 * Java keystore (JKS or PKCS12) and builds a proper SSLContext from it.
 * Encryption and the identity the certificate asserts are both genuine.
 *
 * Client side is honestly incomplete: insecureTrustAllClientContext()
 * accepts ANY certificate the server presents, with no verification at
 * all. That still encrypts the connection against passive eavesdropping,
 * but it does NOT protect against an active man-in-the-middle attacker who
 * can present their own certificate - the client has no way to tell a
 * genuine server from an impostor. Real deployments need a proper
 * truststore (or certificate pinning) wired into the client, which this
 * does not attempt. This distinction - "encrypted" is not the same claim
 * as "verified" - is exactly the kind of thing worth stating plainly
 * rather than letting "TLS support" imply more security than exists.
 */
public final class TlsSupport {
    private TlsSupport() {}

    /** Loads a server SSLContext from a keystore file containing a certificate and its private key. */
    public static SSLContext loadServerContext(String keystorePath, char[] keystorePassword)
            throws GeneralSecurityException, IOException {
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keystorePassword);
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    /**
     * For local development and testing ONLY: trusts any server certificate
     * with zero verification. Never use this against a network you don't
     * fully control - see this class's javadoc for exactly what that means.
     */
    public static SSLContext insecureTrustAllClientContext() throws GeneralSecurityException {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }};
        SSLContext context = SSLContext.getInstance("TLSv1.3");
        context.init(null, trustAllCerts, new SecureRandom());
        return context;
    }

    private static KeyStore loadKeyStore(String path, char[] password) throws GeneralSecurityException, IOException {
        String type = (path.endsWith(".p12") || path.endsWith(".pfx")) ? "PKCS12" : "JKS";
        KeyStore keyStore = KeyStore.getInstance(type);
        try (FileInputStream in = new FileInputStream(path)) {
            keyStore.load(in, password);
        }
        return keyStore;
    }
}
