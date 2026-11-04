package com.stratosdb.benchmark.profiler;

import one.profiler.AsyncProfiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Controller for async-profiler integration and flame graph generation.
 * Enables zero-overhead sampling for CPU and allocation hotspots in StratosDB benchmarks.
 */
public class StdAsyncProfiler {
    private static final Logger LOG = LoggerFactory.getLogger(StdAsyncProfiler.class);

    private static volatile boolean initialized = false;
    private static volatile boolean available = false;
    private static volatile String resolvedLibraryPath = null;
    private static AsyncProfiler profilerInstance = null;

    static {
        init();
    }

    private static synchronized void init() {
        if (initialized) return;
        initialized = true;

        List<String> candidates = new ArrayList<>();
        String envPath = System.getenv("ASYNC_PROFILER_PATH");
        if (envPath != null && !envPath.isBlank()) candidates.add(envPath);

        String envHome = System.getenv("ASYNC_PROFILER_HOME");
        if (envHome != null && !envHome.isBlank()) {
            candidates.add(envHome + "/lib/libasyncProfiler.so");
            candidates.add(envHome + "/build/libasyncProfiler.so");
            candidates.add(envHome + "/libasyncProfiler.so");
        }

        String propPath = System.getProperty("async.profiler.path");
        if (propPath != null && !propPath.isBlank()) candidates.add(propPath);

        // Common IDE / tool installation paths on Linux
        candidates.add("/opt/jetbrains/idea-IU-253.30387.90/lib/async-profiler/amd64/libasyncProfiler.so");
        candidates.add("/opt/jetbrains/DataGrip-2026.1.3/lib/async-profiler/amd64/libasyncProfiler.so");
        candidates.add("/opt/jetbrains/WebStorm-253.30387.83/lib/async-profiler/amd64/libasyncProfiler.so");
        candidates.add("/usr/local/lib/libasyncProfiler.so");
        candidates.add("/usr/lib/libasyncProfiler.so");

        for (String candidate : candidates) {
            File f = new File(candidate);
            if (f.exists() && f.isFile()) {
                try {
                    profilerInstance = AsyncProfiler.getInstance(f.getAbsolutePath());
                    resolvedLibraryPath = f.getAbsolutePath();
                    available = true;
                    LOG.info("StdAsyncProfiler successfully loaded from {}", resolvedLibraryPath);
                    break;
                } catch (Throwable t) {
                    LOG.debug("Candidate async-profiler library failed to load from {}: {}", candidate, t.getMessage());
                }
            }
        }

        if (!available) {
            LOG.debug("async-profiler library not located; flame graph profiling will be disabled");
        }
    }

    public static boolean isAvailable() {
        return available;
    }

    public static String getLibraryPath() {
        return resolvedLibraryPath;
    }

    public static synchronized boolean start(String event) {
        return start(event, 10_000_000L); // 10ms sampling interval default
    }

    public static synchronized boolean start(String event, long intervalNs) {
        if (!available || profilerInstance == null) {
            return false;
        }
        try {
            String evt = (event == null || event.isBlank()) ? "cpu" : event.trim().toLowerCase();
            profilerInstance.execute("start,event=" + evt + ",interval=" + intervalNs);
            LOG.info("async-profiler started with event={} interval={}ns", evt, intervalNs);
            return true;
        } catch (Exception e) {
            LOG.error("Failed to start async-profiler", e);
            return false;
        }
    }

    public static synchronized String stop() {
        if (!available || profilerInstance == null) {
            return "";
        }
        try {
            return profilerInstance.execute("stop");
        } catch (Exception e) {
            LOG.error("Failed to stop async-profiler", e);
            return "";
        }
    }

    public static synchronized boolean dumpFlameGraph(File outputFile) {
        if (!available || profilerInstance == null) {
            return false;
        }
        try {
            if (outputFile.getParentFile() != null && !outputFile.getParentFile().exists()) {
                outputFile.getParentFile().mkdirs();
            }
            profilerInstance.execute("stop,file=" + outputFile.getAbsolutePath());
            LOG.info("async-profiler flame graph written to {}", outputFile.getAbsolutePath());
            return outputFile.exists() && outputFile.length() > 0;
        } catch (Exception e) {
            LOG.error("Failed to dump async-profiler flame graph to {}", outputFile, e);
            return false;
        }
    }

    public static synchronized String execute(String command) {
        if (!available || profilerInstance == null) {
            return "async-profiler unavailable";
        }
        try {
            return profilerInstance.execute(command);
        } catch (Exception e) {
            LOG.error("async-profiler execute failed for command: {}", command, e);
            return "Error: " + e.getMessage();
        }
    }

    public static long getSamples() {
        return (available && profilerInstance != null) ? profilerInstance.getSamples() : 0L;
    }
}
