package com.stratosdb.storage.wal;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * Pluggable destination sink for continuous WAL segment archiving (local filesystem or S3-compatible object store).
 */
public interface WalArchiveSink {
    void uploadSegment(String segmentName, byte[] data) throws IOException;
    byte[] downloadSegment(String segmentName) throws IOException;
    List<String> listSegments() throws IOException;

    /** Local filesystem WAL archive sink with atomic temp-file + rename durability. */
    final class LocalWalArchiveSink implements WalArchiveSink {
        private final Path archiveDir;

        public LocalWalArchiveSink(Path archiveDir) throws IOException {
            this.archiveDir = archiveDir;
            Files.createDirectories(archiveDir);
        }

        @Override
        public void uploadSegment(String segmentName, byte[] data) throws IOException {
            Path tmp = archiveDir.resolve(segmentName + ".tmp");
            Path target = archiveDir.resolve(segmentName);
            Files.write(tmp, data);
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        }

        @Override
        public byte[] downloadSegment(String segmentName) throws IOException {
            return Files.readAllBytes(archiveDir.resolve(segmentName));
        }

        @Override
        public List<String> listSegments() throws IOException {
            if (!Files.exists(archiveDir)) return List.of();
            try (Stream<Path> s = Files.list(archiveDir)) {
                List<String> names = new ArrayList<>();
                s.filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".walseg"))
                    .forEach(names::add);
                Collections.sort(names);
                return names;
            }
        }
    }

    /**
     * S3-compatible HTTP/REST WAL archive sink (AWS S3, MinIO, or compatible gateway)
     * using {@link HttpClient} with SHA-256 content digest and HMAC-SHA256 signing.
     */
    final class S3WalArchiveSink implements WalArchiveSink {
        private static final DateTimeFormatter AMZ_DATE = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);

        private final String endpointBase;
        private final String bucket;
        private final String prefix;
        private final String accessKey;
        private final String secretKey;
        private final HttpClient httpClient;

        public S3WalArchiveSink(String endpointBase, String bucket, String prefix, String accessKey, String secretKey) {
            this.endpointBase = endpointBase.endsWith("/") ? endpointBase.substring(0, endpointBase.length() - 1) : endpointBase;
            this.bucket = bucket;
            this.prefix = (prefix == null || prefix.isEmpty()) ? "" : (prefix.endsWith("/") ? prefix : prefix + "/");
            this.accessKey = accessKey != null ? accessKey : "";
            this.secretKey = secretKey != null ? secretKey : "";
            this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        }

        @Override
        public void uploadSegment(String segmentName, byte[] data) throws IOException {
            String key = prefix + segmentName;
            URI uri = URI.create(endpointBase + "/" + bucket + "/" + key);
            String amzDate = AMZ_DATE.format(Instant.now());
            String payloadHash = sha256Hex(data);
            String sig = hmacSha256Hex(secretKey, "PUT\n/" + bucket + "/" + key + "\n" + amzDate + "\n" + payloadHash);

            HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(30))
                .header("x-amz-date", amzDate)
                .header("x-amz-content-sha256", payloadHash)
                .header("Authorization", "AWS4-HMAC-SHA256 Credential=" + accessKey + ", Signature=" + sig)
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .build();
            try {
                HttpResponse<byte[]> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
                if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                    throw new IOException("S3 PUT failed with status " + resp.statusCode() + " for " + uri);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during S3 WAL upload", e);
            }
        }

        @Override
        public byte[] downloadSegment(String segmentName) throws IOException {
            String key = prefix + segmentName;
            URI uri = URI.create(endpointBase + "/" + bucket + "/" + key);
            HttpRequest req = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30)).GET().build();
            try {
                HttpResponse<byte[]> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofByteArray());
                if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                    throw new IOException("S3 GET failed with status " + resp.statusCode() + " for " + uri);
                }
                return resp.body();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during S3 WAL download", e);
            }
        }

        @Override
        public List<String> listSegments() throws IOException {
            URI uri = URI.create(endpointBase + "/" + bucket + "?prefix=" + prefix);
            HttpRequest req = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(15)).GET().build();
            try {
                HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
                    throw new IOException("S3 LIST failed with status " + resp.statusCode());
                }
                List<String> out = new ArrayList<>();
                for (String line : resp.body().split("\n")) {
                    String trimmed = line.trim();
                    if (trimmed.endsWith(".walseg")) {
                        int slash = trimmed.lastIndexOf('/');
                        out.add(slash >= 0 ? trimmed.substring(slash + 1) : trimmed);
                    }
                }
                Collections.sort(out);
                return out;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during S3 WAL list", e);
            }
        }

        private static String sha256Hex(byte[] data) {
            try {
                byte[] digest = MessageDigest.getInstance("SHA-256").digest(data);
                StringBuilder sb = new StringBuilder(digest.length * 2);
                for (byte b : digest) {
                    sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                    sb.append(Character.forDigit(b & 0xF, 16));
                }
                return sb.toString();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private static String hmacSha256Hex(String key, String data) {
            try {
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
                byte[] sig = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
                StringBuilder sb = new StringBuilder(sig.length * 2);
                for (byte b : sig) {
                    sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                    sb.append(Character.forDigit(b & 0xF, 16));
                }
                return sb.toString();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
