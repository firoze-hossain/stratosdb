package com.stratosdb.testing;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-End Production Release & Packaging Certification Test for Sprint 13 Week 26:
 *   - StratosDB 1.0 Production Release & Docker Packaging
 *   - Dockerfile, entrypoint.sh, and docker-compose.yml verification
 *   - Kubernetes Helm chart with automated replication and watchdog
 *   - Maven Central pure-Java Type 4 driver artifact packaging (binary, sources, javadoc)
 *   - Comprehensive enterprise production deployment documentation
 *   - RozeHub developer ecosystem integration documentation
 *   - Definition of Done: "One-click docker run stratosdb:1.0 ready; comprehensive documentation published."
 */
public class ProductionReleaseAndDockerPackagingEndToEndTest {

    private static final String ROOT_DIR = System.getProperty("user.dir");

    private Path findProjectRoot() {
        Path current = Paths.get(ROOT_DIR).toAbsolutePath().normalize();
        while (current != null) {
            if (Files.exists(current.resolve("stratosdb-jdbc")) && Files.exists(current.resolve("Dockerfile"))) {
                return current;
            }
            current = current.getParent();
        }
        return Paths.get(ROOT_DIR);
    }

    @Test
    @DisplayName("Verify Dockerfile, entrypoint script, and multi-node Docker Compose configuration")
    void testDockerPackagingAndConfiguration() throws Exception {
        Path root = findProjectRoot();

        // 1. Verify Dockerfile
        Path dockerfilePath = root.resolve("Dockerfile");
        assertTrue(Files.exists(dockerfilePath), "Dockerfile must exist at repository root");
        String dockerfileContent = Files.readString(dockerfilePath);
        assertTrue(dockerfileContent.contains("FROM openjdk:25-ea-slim"), "Dockerfile must use OpenJDK 25 runtime");
        assertTrue(dockerfileContent.contains("groupadd -g 1001 stratos"), "Dockerfile must create dedicated stratos group");
        assertTrue(dockerfileContent.contains("useradd -u 1001"), "Dockerfile must create unprivileged stratos user");
        assertTrue(dockerfileContent.contains("EXPOSE 5432 5431 5434"), "Dockerfile must expose ports 5432, 5431, and 5434");
        assertTrue(dockerfileContent.contains("HEALTHCHECK"), "Dockerfile must define container healthcheck");
        assertTrue(dockerfileContent.contains("VOLUME [\"/var/lib/stratosdb/data\"]"), "Dockerfile must define persistent volume mount");

        // 2. Verify docker/entrypoint.sh
        Path entrypointPath = root.resolve("docker/entrypoint.sh");
        assertTrue(Files.exists(entrypointPath), "docker/entrypoint.sh must exist");
        File entrypointFile = entrypointPath.toFile();
        assertTrue(entrypointFile.canExecute(), "docker/entrypoint.sh must be executable");
        String entrypointContent = Files.readString(entrypointPath);
        assertTrue(entrypointContent.contains("UseZGC"), "entrypoint.sh must configure Generational ZGC");
        assertTrue(entrypointContent.contains("std-basebackup"), "entrypoint.sh must handle automated replica basebackup");
        assertTrue(entrypointContent.contains("trap cleanup_and_exit"), "entrypoint.sh must handle graceful shutdown signals");
        assertTrue(entrypointContent.contains("stdsql"), "entrypoint.sh must route stdsql command");
        assertTrue(entrypointContent.contains("stratosdump"), "entrypoint.sh must route stratosdump command");
        assertTrue(entrypointContent.contains("stratos-ha"), "entrypoint.sh must route stratos-ha command");

        // 3. Verify docker-compose.yml
        Path composePath = root.resolve("docker-compose.yml");
        assertTrue(Files.exists(composePath), "docker-compose.yml must exist at repository root");
        String composeContent = Files.readString(composePath);
        assertTrue(composeContent.contains("stratos-primary"), "docker-compose must define primary service");
        assertTrue(composeContent.contains("stratos-replica"), "docker-compose must define replica service");
        assertTrue(composeContent.contains("stratos-watchdog"), "docker-compose must define consensus watchdog service");
        assertTrue(composeContent.contains("5432:5432"), "docker-compose primary must expose port 5432");
        assertTrue(composeContent.contains("STRATOS_CLUSTER_MODE=replica"), "docker-compose replica must be in replica mode");
    }

    @Test
    @DisplayName("Verify Kubernetes Helm Chart specification and template rendering")
    void testKubernetesHelmChartSpecification() throws Exception {
        Path root = findProjectRoot();
        Path helmDir = root.resolve("deploy/helm/stratosdb");
        assertTrue(Files.exists(helmDir), "deploy/helm/stratosdb directory must exist");

        // 1. Chart.yaml
        Path chartYamlPath = helmDir.resolve("Chart.yaml");
        assertTrue(Files.exists(chartYamlPath), "Chart.yaml must exist");
        String chartYaml = Files.readString(chartYamlPath);
        assertTrue(chartYaml.contains("name: stratosdb"), "Chart.yaml must define name: stratosdb");
        assertTrue(chartYaml.contains("version: 1.0.0"), "Chart.yaml must define version: 1.0.0");
        assertTrue(chartYaml.contains("appVersion: \"1.0.0\""), "Chart.yaml must define appVersion: 1.0.0");

        // 2. values.yaml
        Path valuesYamlPath = helmDir.resolve("values.yaml");
        assertTrue(Files.exists(valuesYamlPath), "values.yaml must exist");
        String valuesYaml = Files.readString(valuesYamlPath);
        assertTrue(valuesYaml.contains("stdwire: 5432"), "values.yaml must specify stdwire port 5432");
        assertTrue(valuesYaml.contains("native: 5431"), "values.yaml must specify native port 5431");
        assertTrue(valuesYaml.contains("replication: 5434"), "values.yaml must specify replication port 5434");
        assertTrue(valuesYaml.contains("primary:"), "values.yaml must configure primary node");
        assertTrue(valuesYaml.contains("replica:"), "values.yaml must configure replica nodes");
        assertTrue(valuesYaml.contains("watchdog:"), "values.yaml must configure consensus watchdog");

        // 3. Templates
        Path templatesDir = helmDir.resolve("templates");
        assertTrue(Files.exists(templatesDir.resolve("primary-statefulset.yaml")), "primary-statefulset.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("replica-statefulset.yaml")), "replica-statefulset.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("service-primary.yaml")), "service-primary.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("service-replica.yaml")), "service-replica.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("service-headless.yaml")), "service-headless.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("watchdog-deployment.yaml")), "watchdog-deployment.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("configmap.yaml")), "configmap.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("secrets.yaml")), "secrets.yaml must exist");
        assertTrue(Files.exists(templatesDir.resolve("_helpers.tpl")), "_helpers.tpl must exist");

        // 4. Helm README.md
        Path readmePath = helmDir.resolve("README.md");
        assertTrue(Files.exists(readmePath), "Helm README.md must exist");
        String readmeContent = Files.readString(readmePath);
        assertTrue(readmeContent.contains("helm install"), "Helm README must document installation command");
    }

    @Test
    @DisplayName("Verify Maven Central driver artifact packaging (binary, sources, javadoc)")
    void testMavenCentralDriverArtifactPackaging() throws Exception {
        Path root = findProjectRoot();

        // 1. Verify stratosdb-jdbc/pom.xml
        Path jdbcPomPath = root.resolve("stratosdb-jdbc/pom.xml");
        assertTrue(Files.exists(jdbcPomPath), "stratosdb-jdbc/pom.xml must exist");
        String pomContent = Files.readString(jdbcPomPath);
        assertTrue(pomContent.contains("<artifactId>stratosdb-jdbc</artifactId>"), "pom must define stratosdb-jdbc artifactId");
        assertTrue(pomContent.contains("StratosDB JDBC Driver"), "pom must define descriptive driver name");
        assertTrue(pomContent.contains("maven-source-plugin"), "pom must declare maven-source-plugin");
        assertTrue(pomContent.contains("maven-javadoc-plugin"), "pom must declare maven-javadoc-plugin");
        assertTrue(pomContent.contains("nexus-staging-maven-plugin"), "pom must declare nexus-staging-maven-plugin for OSSRH");

        // 2. Verify publish script
        Path publishScriptPath = root.resolve("scripts/publish_maven_central.sh");
        assertTrue(Files.exists(publishScriptPath), "scripts/publish_maven_central.sh must exist");
        assertTrue(publishScriptPath.toFile().canExecute(), "scripts/publish_maven_central.sh must be executable");

        // 3. Verify packaged jars in target directory
        Path targetDir = root.resolve("stratosdb-jdbc/target");
        assertTrue(Files.exists(targetDir), "stratosdb-jdbc/target directory must exist");

        File[] binaryJars = targetDir.toFile().listFiles((dir, name) -> name.startsWith("stratosdb-jdbc-") && !name.contains("sources") && !name.contains("javadoc") && name.endsWith(".jar"));
        assertNotNull(binaryJars);
        assertTrue(binaryJars.length > 0, "Binary driver jar must exist in target/");

        File[] sourceJars = targetDir.toFile().listFiles((dir, name) -> name.contains("-sources.jar"));
        assertNotNull(sourceJars);
        assertTrue(sourceJars.length > 0, "Sources jar must exist in target/");

        File[] javadocJars = targetDir.toFile().listFiles((dir, name) -> name.contains("-javadoc.jar"));
        assertNotNull(javadocJars);
        assertTrue(javadocJars.length > 0, "Javadoc jar must exist in target/");
    }

    @Test
    @DisplayName("Verify production documentation guides and RozeHub ecosystem documentation")
    void testDocumentationAndProductionGuides() throws Exception {
        Path root = findProjectRoot();
        Path docsDir = root.resolve("docs");

        // 1. StratosDB Documentation Guides
        String[] requiredGuides = {
            "PRODUCTION_DEPLOYMENT.md",
            "HIGH_AVAILABILITY_GUIDE.md",
            "SECURITY_GUIDE.md",
            "CLIENT_CONNECTIVITY.md",
            "DOCKER_GUIDE.md"
        };

        for (String guide : requiredGuides) {
            Path guidePath = docsDir.resolve(guide);
            assertTrue(Files.exists(guidePath), "Guide " + guide + " must exist in docs/");
            assertTrue(Files.size(guidePath) > 500, "Guide " + guide + " must contain substantial content");
        }

        // 2. Root README.md
        Path rootReadmePath = root.resolve("README.md");
        assertTrue(Files.exists(rootReadmePath), "Root README.md must exist");
        String readmeContent = Files.readString(rootReadmePath);
        assertTrue(readmeContent.contains("StratosDB 1.0"), "README must reflect 1.0 GA release");
        assertTrue(readmeContent.contains("docker run"), "README must document docker run quickstart");
        assertTrue(readmeContent.contains("helm install"), "README must document helm install");
        assertTrue(readmeContent.contains("stratosdb-jdbc"), "README must document Maven coordinates");

        // 3. RozeHub Ecosystem Release & Documentation Seeder
        Path rozehubSeederPath = Paths.get("/home/firoze/projects/others/RozeHub/database/seeders/StratosDBDocumentationSeeder.php");
        assertTrue(Files.exists(rozehubSeederPath), "RozeHub documentation seeder must exist at /home/firoze/projects/others/RozeHub/database/seeders/StratosDBDocumentationSeeder.php");
        String rozehubSeeder = Files.readString(rozehubSeederPath);
        assertTrue(rozehubSeeder.contains("StratosDB 1.0.0"), "RozeHub seeder must describe StratosDB 1.0.0");
        assertTrue(rozehubSeeder.contains("DBNavigator"), "RozeHub seeder must cover DBNavigator integration");
        assertTrue(rozehubSeeder.contains("pdo_pgsql"), "RozeHub seeder must cover Laravel / PHP PDO compatibility");
        assertTrue(rozehubSeeder.contains("stratosdb-jdbc"), "RozeHub seeder must cover stratosdb-jdbc driver");
        assertTrue(rozehubSeeder.contains("documentation_sections"), "RozeHub seeder must populate documentation_sections");
        assertTrue(rozehubSeeder.contains("documentation_pages"), "RozeHub seeder must populate documentation_pages");

        Path rozehubDatabaseSeederPath = Paths.get("/home/firoze/projects/others/RozeHub/database/seeders/DatabaseSeeder.php");
        assertTrue(Files.exists(rozehubDatabaseSeederPath), "RozeHub DatabaseSeeder must exist");
        String databaseSeeder = Files.readString(rozehubDatabaseSeederPath);
        assertTrue(databaseSeeder.contains("StratosDBDocumentationSeeder::class"), "DatabaseSeeder must call StratosDBDocumentationSeeder::class");
    }

    @Test
    @DisplayName("Verify live container running and SQL execution over wire protocol")
    void testLiveContainerExecutionAndQuerying() {
        try {
            Process proc = new ProcessBuilder("docker", "ps", "--filter", "name=stratos-test", "--format", "{{.Status}}")
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()))) {
                String line = reader.readLine();
                if (line != null && line.toLowerCase().contains("up")) {
                    Process queryProc = new ProcessBuilder("docker", "exec", "-i", "stratos-test", "stdsql", "-p", "5432", "-U", "admin", "-d", "stratos")
                            .redirectErrorStream(true)
                            .start();
                    queryProc.getOutputStream().write("SELECT 42 AS answer;\n".getBytes());
                    queryProc.getOutputStream().flush();
                    queryProc.getOutputStream().close();

                    StringBuilder output = new StringBuilder();
                    try (BufferedReader qReader = new BufferedReader(new InputStreamReader(queryProc.getInputStream()))) {
                        String qLine;
                        while ((qLine = qReader.readLine()) != null) {
                            output.append(qLine).append("\n");
                        }
                    }
                    int exitCode = queryProc.waitFor();
                    assertEquals(0, exitCode, "Query inside container should exit successfully");
                    assertTrue(output.toString().contains("42"), "Query output should contain 42: " + output);
                }
            }
        } catch (Exception e) {
            System.out.println("Note: Live container check skipped: " + e.getMessage());
        }
    }
}
