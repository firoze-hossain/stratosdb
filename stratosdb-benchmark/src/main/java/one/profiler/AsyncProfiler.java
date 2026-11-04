package one.profiler;

import java.io.IOException;

/**
 * Standard JNI bridge for async-profiler.
 * Matches the native method signatures exported by libasyncProfiler.so / .dylib / .dll.
 */
public class AsyncProfiler {
    private static volatile AsyncProfiler instance;

    public static synchronized AsyncProfiler getInstance(String libPath) {
        if (instance == null) {
            System.load(libPath);
            instance = new AsyncProfiler();
        }
        return instance;
    }

    public static AsyncProfiler getInstance() {
        return instance;
    }

    public native void start0(String event, long interval, boolean reset) throws IllegalStateException;
    public native void stop0() throws IllegalStateException;
    public native String execute0(String command) throws IllegalArgumentException, IOException;
    public native long getSamples();

    public String execute(String command) throws IOException {
        return execute0(command);
    }
}
