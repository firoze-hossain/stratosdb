package com.stratosdb.sql.extension;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * The one, fixed JNI bridge this engine ever loads via System.load() -
 * every "extension" (an arbitrary, user-supplied native shared library)
 * is loaded not through JNI's own class-based native-method mechanism
 * directly, but through this bridge's own real dlopen()/dlsym() calls at
 * runtime. This is the actual reason a real, working, multi-extension
 * system is possible at all here: JNI's own convention binds a native
 * method to a fixed, compile-time-known C symbol name
 * (Java_ClassName_methodName) - if two different extensions' own shared
 * libraries each tried to export that same, fixed symbol name to be
 * loaded directly via System.load(), the second one to load would
 * either fail outright or silently shadow the first, depending on the
 * platform's dynamic linker. Loading extensions through this bridge's
 * own dlopen()/dlsym() calls instead sidesteps that entirely - the same
 * real mechanism real Postgres's own C extensions use to load a shared
 * library and look up a named symbol in it, just reached from Java
 * through one, fixed, already-proven JNI entry point rather than
 * Postgres's own fmgr.
 *
 * Real, honestly-stated scope and limitations:
 *   - Requires libstratosbridge.so to already be built - see
 *     native/README.md and native/build.sh in this repo. This is a
 *     real, separate, one-time native build step outside Maven's own
 *     build (Maven does not compile C code) - StratosDB itself runs
 *     completely fine with no extensions and without ever building
 *     this bridge at all; it's only needed the moment CREATE EXTENSION
 *     is actually used.
 *   - Integer-only calling convention: every native extension function
 *     must have the real C signature `int64_t name(int64_t* args, int32_t argc)`
 *     - this bridge's own invokeIntFunction call assumes exactly that
 *     ABI. No strings, floats, or any other SQL type can cross this
 *     boundary yet - a real, named limitation, not an oversight.
 *   - A loaded native library can never be unloaded within the same
 *     JVM process - this is a real, well-known property of dlopen
 *     within a long-running process, not something this bridge can work
 *     around. DROP EXTENSION removes this engine's own SQL-level
 *     registration (so the extension's functions stop being callable),
 *     but the shared library itself stays mapped into this process
 *     until the JVM itself exits.
 *   - No sandboxing whatsoever. A loaded extension's native code runs
 *     with the exact same privileges as the StratosDB process itself -
 *     a memory-unsafe or malicious extension can corrupt or crash the
 *     entire database process, or do anything else native code can do
 *     on this machine. CREATE EXTENSION should be treated the same way
 *     as running arbitrary native code, because that is exactly what it
 *     does - real Postgres's own C extensions carry this same risk.
 */
public class NativeExtensionBridge {
    private static final Logger LOG = LoggerFactory.getLogger(NativeExtensionBridge.class);
    private static volatile boolean bridgeLoaded = false;
    private static volatile Throwable loadFailure = null;

    private static synchronized void ensureBridgeLoaded() {
        if (bridgeLoaded) return;
        if (loadFailure != null) {
            throw new IllegalStateException("Native extension bridge previously failed to load: " + loadFailure.getMessage(), loadFailure);
        }
        String path = findBridgeLibrary();
        try {
            System.load(path);
            bridgeLoaded = true;
            LOG.info("Native extension bridge loaded from {}", path);
        } catch (Throwable t) {
            loadFailure = t;
            throw new IllegalStateException("Failed to load native extension bridge from " + path
                + " - see native/README.md for how to build it. Underlying error: " + t.getMessage(), t);
        }
    }

    /** Looks in a few conventional locations relative to the working directory and the STRATOSDB_NATIVE_DIR environment variable, rather than assuming one fixed, hardcoded path - a real, separate native build step (see native/build.sh) can place the built library in whichever of these is most convenient. */
    private static String findBridgeLibrary() {
        String libName = System.mapLibraryName("stratosbridge"); // libstratosbridge.so on Linux, .dylib on macOS
        String envDir = System.getenv("STRATOSDB_NATIVE_DIR");
        String[] candidates = envDir != null
            ? new String[]{envDir + "/" + libName, "./native/" + libName, "./" + libName}
            : new String[]{"./native/" + libName, "./" + libName};
        for (String candidate : candidates) {
            if (new File(candidate).exists()) {
                return new File(candidate).getAbsolutePath();
            }
        }
        throw new IllegalStateException("Could not find " + libName + " in any of: " + String.join(", ", candidates)
            + " - build it first (see native/README.md), or set STRATOSDB_NATIVE_DIR to the directory containing it.");
    }

    /** Real dlopen(path, RTLD_NOW) under the hood. Returns a native handle (an opaque pointer value, not a Java object) - 0 means dlopen failed; check the process's own stderr for dlerror()'s message, since JNI has no clean way to propagate a C errno-style error back as a Java exception from this call itself. */
    public static long loadLibrary(String path) {
        ensureBridgeLoaded();
        return dlopenLibrary(path);
    }

    /** Real dlsym(handle, symbolName) under the hood. Returns a function pointer value (0 means the symbol was not found in that library). */
    public static long lookupSymbol(long handle, String symbolName) {
        ensureBridgeLoaded();
        return dlsymFunction(handle, symbolName);
    }

    /** Calls the given function pointer as if it had the real C signature `int64_t name(int64_t* args, int32_t argc)` - see this class's own javadoc for why this integer-only convention is this first version's real, stated scope. */
    public static long invoke(long funcPtr, long[] args) {
        ensureBridgeLoaded();
        return invokeIntFunction(funcPtr, args);
    }

    private static native long dlopenLibrary(String path);
    private static native long dlsymFunction(long handle, String symbolName);
    private static native long invokeIntFunction(long funcPtr, long[] args);
}
