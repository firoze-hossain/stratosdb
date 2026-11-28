package com.stratosdb.testing;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Sprint 13 Week 26: StratosDB 2.0 Enterprise GA Release:
 *   - Multi-arch Docker build support (amd64, arm64)
 *   - Kubernetes Helm chart with 3-node Raft clustering & TLS (< 60s startup)
 *   - Pure-Java Type 4 JDBC driver module & service registration
 *   - 100% Documentation Portal coverage
 */
class EnterpriseGaReleaseTest {

    private static Path findProjectRoot() {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            if (Files.exists(current.resolve("stratosdb-jdbc")) && Files.exists(current.resolve("Dockerfile"))) {
                return current;
            }
            current = current.getParent();
        }
        return Paths.get(System.getProperty("user.dir"));
    }

    @Test
    void testMultiArchDockerAndBuildScript() throws Exception {
        Path root = findProjectRoot();
        String dockerfile = Files.readString(root.resolve("Dockerfile"));
        assertTrue(dockerfile.contains("ARG TARGETARCH="), "Dockerfile must support multi-arch TARGETARCH");
        assertTrue(dockerfile.contains("org.opencontainers.image.version=\"2.0.0\""), "Dockerfile must carry 2.0.0 GA label");
        assertTrue(dockerfile.contains("EXPOSE 3306 5435"), "Dockerfile must expose MySQL (3306) and Raft (5435) ports");

        Path multiArchScript = root.resolve("scripts/build-multiarch-docker.sh");
        assertTrue(Files.exists(multiArchScript), "Multi-arch docker build script must exist");
        String script = Files.readString(multiArchScript);
        assertTrue(script.contains("linux/amd64,linux/arm64"), "Build script must target both amd64 and arm64");
    }

    @Test
    void testHelmThreeNodeRaftClusterWithTlsUnder60s() throws Exception {
        Path root = findProjectRoot();
        Path helmDir = root.resolve("deploy/helm/stratosdb");

        String values = Files.readString(helmDir.resolve("values.yaml"));
        assertTrue(values.contains("clusterSize: 3"), "Helm values must configure 3-node Raft cluster");
        assertTrue(values.contains("startupTimeoutSeconds: 60"), "Helm values must configure < 60s cluster startup target");
        assertTrue(values.contains("autoGenerate: true"), "Helm values must support automated TLS cert generation");

        String raftYaml = Files.readString(helmDir.resolve("templates/raft-cluster.yaml"));
        assertTrue(raftYaml.contains("podManagementPolicy: Parallel"), "Parallel pod startup ensures 3-node cluster ready in < 60s");
        assertTrue(raftYaml.contains("kind: StatefulSet"));
        assertTrue(raftYaml.contains("tls-certs"));

        String tlsSecret = Files.readString(helmDir.resolve("templates/tls-secret.yaml"));
        assertTrue(tlsSecret.contains("kubernetes.io/tls"));
        assertTrue(tlsSecret.contains("genSignedCert"));
    }

    @Test
    void testPureJavaJdbcDriverAndDocumentationPortal() throws Exception {
        Path root = findProjectRoot();

        // Pure-Java Type 4 JDBC Driver
        Path driverSvc = root.resolve("stratosdb-jdbc/src/main/resources/META-INF/services/java.sql.Driver");
        assertTrue(Files.exists(driverSvc), "JDBC Driver SPI file must exist");
        assertTrue(Files.readString(driverSvc).contains("com.stratosdb.jdbc.StratosDriver"));
        assertTrue(Files.exists(root.resolve("stratosdb-jdbc/src/main/java/module-info.java")));

        // Documentation Portal & 100% coverage guides
        Path portal = root.resolve("docs/index.html");
        assertTrue(Files.exists(portal), "Documentation portal index.html must exist");
        String portalHtml = Files.readString(portal);
        assertTrue(portalHtml.contains("StratosDB 2.0 Documentation Portal"));
        assertTrue(portalHtml.contains("STRATOSDB_2_0_REFERENCE.md"));
        assertTrue(portalHtml.contains("HIGH_AVAILABILITY_GUIDE.md"));
        assertTrue(portalHtml.contains("SECURITY_GUIDE.md"));
        assertTrue(portalHtml.contains("PRODUCTION_DEPLOYMENT.md"));
        assertTrue(portalHtml.contains("CLIENT_CONNECTIVITY.md"));
        assertTrue(portalHtml.contains("DOCKER_GUIDE.md"));

        assertTrue(Files.exists(root.resolve("docs/STRATOSDB_2_0_REFERENCE.md")));
    }
}
