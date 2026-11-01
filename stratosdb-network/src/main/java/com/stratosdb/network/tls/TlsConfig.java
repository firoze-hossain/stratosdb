package com.stratosdb.network.tls;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * Configuration options for TLS/SSL integration in {@link com.stratosdb.network.stdwire.StdWireServer}.
 *
 * Supports:
 * - TLS 1.3 and TLS 1.2 protocols
 * - Configurable Keystore (server certificate and private key)
 * - Configurable Truststore (CA / client certificate verification)
 * - ALPN (Application-Layer Protocol Negotiation, e.g. "postgresql", "stratosdb")
 * - Client certificate authentication (mTLS with NONE, WANT, NEED modes)
 */
public class TlsConfig {

    public enum ClientAuth {
        NONE,
        WANT,
        NEED
    }

    private boolean enabled = true;
    private boolean requireTls = false;

    private String keystorePath;
    private char[] keystorePassword;
    private String keystoreType = "PKCS12";
    private char[] keyPassword;

    private String truststorePath;
    private char[] truststorePassword;
    private String truststoreType = "PKCS12";

    private ClientAuth clientAuth = ClientAuth.NONE;
    private boolean clientCertAuth = false;

    private String[] protocols = new String[]{"TLSv1.3", "TLSv1.2"};
    private String[] applicationProtocols = new String[]{"postgresql", "stratosdb"};

    private SSLContext sslContext;

    public TlsConfig() {}

    public static TlsConfig disabled() {
        TlsConfig config = new TlsConfig();
        config.setEnabled(false);
        return config;
    }

    public static TlsConfig fromKeystore(String keystorePath, char[] keystorePassword) {
        TlsConfig config = new TlsConfig();
        config.setEnabled(true);
        config.setKeystorePath(keystorePath);
        config.setKeystorePassword(keystorePassword);
        return config;
    }

    public static TlsConfig fromSslContext(SSLContext sslContext) {
        TlsConfig config = new TlsConfig();
        config.setEnabled(true);
        config.setSslContext(sslContext);
        return config;
    }

    public synchronized SSLContext getOrCreateSslContext() throws GeneralSecurityException, IOException {
        if (sslContext != null) {
            return sslContext;
        }
        if (!enabled) {
            return null;
        }
        if (keystorePath != null) {
            sslContext = TlsSupport.loadServerContext(
                keystorePath, keystorePassword, keystoreType, keyPassword,
                truststorePath, truststorePassword, truststoreType
            );
            return sslContext;
        }
        return null;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TlsConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public boolean isRequireTls() {
        return requireTls;
    }

    public TlsConfig setRequireTls(boolean requireTls) {
        this.requireTls = requireTls;
        return this;
    }

    public String getKeystorePath() {
        return keystorePath;
    }

    public TlsConfig setKeystorePath(String keystorePath) {
        this.keystorePath = keystorePath;
        return this;
    }

    public char[] getKeystorePassword() {
        return keystorePassword;
    }

    public TlsConfig setKeystorePassword(char[] keystorePassword) {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public TlsConfig setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
        return this;
    }

    public char[] getKeyPassword() {
        return keyPassword != null ? keyPassword : keystorePassword;
    }

    public TlsConfig setKeyPassword(char[] keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public TlsConfig setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
        return this;
    }

    public char[] getTruststorePassword() {
        return truststorePassword;
    }

    public TlsConfig setTruststorePassword(char[] truststorePassword) {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public TlsConfig setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
        return this;
    }

    public ClientAuth getClientAuth() {
        return clientAuth;
    }

    public TlsConfig setClientAuth(ClientAuth clientAuth) {
        this.clientAuth = clientAuth;
        return this;
    }

    public boolean isClientCertAuth() {
        return clientCertAuth;
    }

    public TlsConfig setClientCertAuth(boolean clientCertAuth) {
        this.clientCertAuth = clientCertAuth;
        if (clientCertAuth && this.clientAuth == ClientAuth.NONE) {
            this.clientAuth = ClientAuth.NEED;
        }
        return this;
    }

    public String[] getProtocols() {
        return protocols != null ? protocols.clone() : null;
    }

    public TlsConfig setProtocols(String[] protocols) {
        this.protocols = protocols != null ? protocols.clone() : null;
        return this;
    }

    public String[] getApplicationProtocols() {
        return applicationProtocols != null ? applicationProtocols.clone() : null;
    }

    public TlsConfig setApplicationProtocols(String[] applicationProtocols) {
        this.applicationProtocols = applicationProtocols != null ? applicationProtocols.clone() : null;
        return this;
    }

    public SSLContext getSslContext() {
        return sslContext;
    }

    public TlsConfig setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
        this.enabled = (sslContext != null);
        return this;
    }

    @Override
    public String toString() {
        return "TlsConfig{" +
                "enabled=" + enabled +
                ", requireTls=" + requireTls +
                ", keystorePath='" + keystorePath + '\'' +
                ", truststorePath='" + truststorePath + '\'' +
                ", clientAuth=" + clientAuth +
                ", clientCertAuth=" + clientCertAuth +
                ", protocols=" + Arrays.toString(protocols) +
                ", applicationProtocols=" + Arrays.toString(applicationProtocols) +
                '}';
    }
}
