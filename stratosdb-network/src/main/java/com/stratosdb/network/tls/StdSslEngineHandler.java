package com.stratosdb.network.tls;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles the SSLEngine TLS 1.3 handshake and provides encrypted stream adapters
 * for blocking socket I/O in {@link com.stratosdb.network.stdwire.StdWireServer}.
 *
 * Implements:
 * - Full SSLEngine state machine (NEED_WRAP, NEED_UNWRAP, NEED_TASK)
 * - ALPN (Application-Layer Protocol Negotiation)
 * - Client certificate authentication (mTLS)
 * - Transparent wrapping of socket input/output streams
 */
public class StdSslEngineHandler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdSslEngineHandler.class);

    private final Socket socket;
    private final InputStream rawIn;
    private final OutputStream rawOut;
    private final SSLEngine engine;
    private final TlsConfig config;

    private final ByteBuffer netIn;
    private final ByteBuffer netOut;
    private final ByteBuffer appIn;
    private final ByteBuffer appOut;

    private final SslEngineInputStream sslIn;
    private final SslEngineOutputStream sslOut;

    private final byte[] readBuffer = new byte[8192];
    private boolean handshakeDone = false;

    public StdSslEngineHandler(Socket socket, InputStream rawIn, OutputStream rawOut, SSLContext sslContext, TlsConfig config) {
        this.socket = socket;
        this.rawIn = rawIn;
        this.rawOut = rawOut;
        this.config = config;

        this.engine = sslContext.createSSLEngine();
        this.engine.setUseClientMode(false); // Server mode

        if (config != null) {
            if (config.getClientAuth() == TlsConfig.ClientAuth.NEED) {
                this.engine.setNeedClientAuth(true);
            } else if (config.getClientAuth() == TlsConfig.ClientAuth.WANT) {
                this.engine.setWantClientAuth(true);
            }

            SSLParameters params = this.engine.getSSLParameters();
            if (config.getProtocols() != null && config.getProtocols().length > 0) {
                params.setProtocols(config.getProtocols());
            }
            if (config.getApplicationProtocols() != null && config.getApplicationProtocols().length > 0) {
                params.setApplicationProtocols(config.getApplicationProtocols());
            }
            this.engine.setSSLParameters(params);
        }

        SSLSession session = this.engine.getSession();
        int netBufferSize = session.getPacketBufferSize();
        int appBufferSize = session.getApplicationBufferSize();

        this.netIn = ByteBuffer.allocate(Math.max(netBufferSize, 32768));
        this.netOut = ByteBuffer.allocate(Math.max(netBufferSize, 32768));
        this.appIn = ByteBuffer.allocate(Math.max(appBufferSize, 32768));
        this.appOut = ByteBuffer.allocate(Math.max(appBufferSize, 32768));

        this.appIn.position(0);
        this.appIn.limit(0); // Initially empty

        this.sslIn = new SslEngineInputStream();
        this.sslOut = new SslEngineOutputStream();
    }

    /**
     * Executes the SSLEngine handshake state machine synchronously.
     */
    public void performHandshake() throws IOException {
        engine.beginHandshake();
        SSLEngineResult.HandshakeStatus status = engine.getHandshakeStatus();

        while (status != SSLEngineResult.HandshakeStatus.FINISHED &&
               status != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

            switch (status) {
                case NEED_WRAP -> {
                    netOut.clear();
                    SSLEngineResult result = engine.wrap(appOut, netOut);
                    status = result.getHandshakeStatus();
                    netOut.flip();
                    if (netOut.hasRemaining()) {
                        rawOut.write(netOut.array(), netOut.arrayOffset() + netOut.position(), netOut.remaining());
                        rawOut.flush();
                    }
                }
                case NEED_UNWRAP -> {
                    if (netIn.position() == 0) {
                        int read = rawIn.read(readBuffer);
                        if (read == -1) {
                            throw new EOFException("Connection closed by peer during TLS handshake");
                        }
                        netIn.put(readBuffer, 0, read);
                    }
                    netIn.flip();
                    SSLEngineResult result = engine.unwrap(netIn, appIn);
                    netIn.compact();
                    status = result.getHandshakeStatus();

                    if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                        int read = rawIn.read(readBuffer);
                        if (read == -1) {
                            throw new EOFException("Connection closed by peer during TLS handshake");
                        }
                        netIn.put(readBuffer, 0, read);
                    }
                }
                case NEED_TASK -> {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    status = engine.getHandshakeStatus();
                }
                default -> {}
            }
        }
        handshakeDone = true;
        LOG.debug("TLS handshake complete: protocol={}, cipher={}, ALPN={}",
                engine.getSession().getProtocol(), engine.getSession().getCipherSuite(), engine.getApplicationProtocol());
    }

    public InputStream getInputStream() {
        return sslIn;
    }

    public OutputStream getOutputStream() {
        return sslOut;
    }

    public SSLEngine getEngine() {
        return engine;
    }

    public SSLSession getSession() {
        return engine.getSession();
    }

    public String getApplicationProtocol() {
        return engine.getApplicationProtocol();
    }

    public boolean isHandshakeDone() {
        return handshakeDone;
    }

    public X509Certificate getPeerCertificate() {
        try {
            Certificate[] certs = engine.getSession().getPeerCertificates();
            if (certs != null && certs.length > 0 && certs[0] instanceof X509Certificate x509) {
                return x509;
            }
        } catch (SSLPeerUnverifiedException ignored) {
        }
        return null;
    }

    private byte[] tlsUniqueBytes;

    public void setTlsUnique(byte[] bytes) {
        this.tlsUniqueBytes = bytes;
    }

    public X509Certificate getLocalCertificate() {
        try {
            Certificate[] certs = engine.getSession().getLocalCertificates();
            if (certs != null && certs.length > 0 && certs[0] instanceof X509Certificate x509) {
                return x509;
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public byte[] getTlsServerEndPoint() {
        X509Certificate cert = getLocalCertificate();
        if (cert == null) return null;
        try {
            return MessageDigest.getInstance("SHA-256").digest(cert.getEncoded());
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 algorithm must be available", e);
        }
    }

    public byte[] getTlsUnique() {
        if (tlsUniqueBytes != null) {
            return tlsUniqueBytes;
        }
        try {
            if (engine.getSession() instanceof ExtendedSSLSession extSession) {
                return extSession.exportKeyingMaterialData("EXPORTER-Channel-Binding", new byte[0], 32);
            }
        } catch (Exception ignored) {
        }
        byte[] id = engine.getSession().getId();
        if (id != null && id.length > 0) {
            try {
                return MessageDigest.getInstance("SHA-256").digest(id);
            } catch (Exception ignored) {
            }
        }
        return null;
    }

    public Map<String, byte[]> getChannelBindings() {
        Map<String, byte[]> map = new HashMap<>();
        byte[] endPoint = getTlsServerEndPoint();
        if (endPoint != null) {
            map.put("tls-server-end-point", endPoint);
        }
        byte[] unique = getTlsUnique();
        if (unique != null) {
            map.put("tls-unique", unique);
        }
        return Collections.unmodifiableMap(map);
    }

    @Override
    public void close() throws IOException {
        try {
            engine.closeOutbound();
            netOut.clear();
            SSLEngineResult result = engine.wrap(ByteBuffer.allocate(0), netOut);
            netOut.flip();
            if (netOut.hasRemaining()) {
                rawOut.write(netOut.array(), netOut.arrayOffset() + netOut.position(), netOut.remaining());
                rawOut.flush();
            }
        } catch (Exception ignored) {
        } finally {
            socket.close();
        }
    }

    private class SslEngineInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int r = read(one, 0, 1);
            return (r == -1) ? -1 : (one[0] & 0xFF);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) return 0;

            while (true) {
                if (appIn.hasRemaining()) {
                    int toCopy = Math.min(len, appIn.remaining());
                    appIn.get(b, off, toCopy);
                    return toCopy;
                }

                appIn.clear();

                if (netIn.position() == 0) {
                    int read = rawIn.read(readBuffer);
                    if (read == -1) {
                        return -1;
                    }
                    netIn.put(readBuffer, 0, read);
                }

                netIn.flip();
                SSLEngineResult result = engine.unwrap(netIn, appIn);
                netIn.compact();

                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                    Runnable task;
                    while ((task = engine.getDelegatedTask()) != null) {
                        task.run();
                    }
                }

                if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_WRAP) {
                    netOut.clear();
                    SSLEngineResult wrapRes = engine.wrap(ByteBuffer.allocate(0), netOut);
                    netOut.flip();
                    if (netOut.hasRemaining()) {
                        rawOut.write(netOut.array(), netOut.arrayOffset() + netOut.position(), netOut.remaining());
                        rawOut.flush();
                    }
                }

                if (result.getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                    int read = rawIn.read(readBuffer);
                    if (read == -1) {
                        return -1;
                    }
                    netIn.put(readBuffer, 0, read);
                    continue;
                }

                if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                    return -1;
                }

                appIn.flip();
                if (appIn.hasRemaining()) {
                    int toCopy = Math.min(len, appIn.remaining());
                    appIn.get(b, off, toCopy);
                    return toCopy;
                }
            }
        }
    }

    private class SslEngineOutputStream extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            byte[] one = new byte[]{(byte) b};
            write(one, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int remaining = len;
            int offset = off;

            while (remaining > 0) {
                int chunkSize = Math.min(remaining, 16384);
                ByteBuffer chunk = ByteBuffer.wrap(b, offset, chunkSize);
                netOut.clear();
                SSLEngineResult result = engine.wrap(chunk, netOut);

                if (result.getStatus() == SSLEngineResult.Status.CLOSED) {
                    throw new IOException("SSLEngine is closed");
                }

                netOut.flip();
                if (netOut.hasRemaining()) {
                    rawOut.write(netOut.array(), netOut.arrayOffset() + netOut.position(), netOut.remaining());
                }

                int consumed = chunkSize - chunk.remaining();
                offset += consumed;
                remaining -= consumed;
            }
        }

        @Override
        public void flush() throws IOException {
            rawOut.flush();
        }

        @Override
        public void close() throws IOException {
            StdSslEngineHandler.this.close();
        }
    }
}
