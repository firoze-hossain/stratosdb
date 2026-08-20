/*
 * StratosDB's fixed native extension bridge - see
 * NativeExtensionBridge.java's own javadoc (stratosdb-sql module) for
 * why this bridge exists and its real, honestly-stated scope and
 * limitations.
 *
 * This is the ONLY native library StratosDB itself ever loads via JNI's
 * own System.load() mechanism. It exists purely to expose dlopen()/
 * dlsym() to Java, so that arbitrary, separate extension .so files can
 * be loaded and called at runtime WITHOUT each one needing to implement
 * JNI's own class-based native-method binding convention itself (which
 * would collide the moment two different extensions tried to export the
 * same fixed Java_ClassName_methodName symbol).
 *
 * Build: see build.sh in this same directory. Requires gcc and the
 * JDK's own jni.h/jni_md.h headers (part of any standard JDK install,
 * matched here via $JAVA_HOME).
 */
#include "com_stratosdb_sql_extension_NativeExtensionBridge.h"
#include <dlfcn.h>
#include <stdint.h>
#include <stdio.h>

typedef int64_t (*stratos_ext_func_t)(int64_t *args, int32_t argc);

JNIEXPORT jlong JNICALL Java_com_stratosdb_sql_extension_NativeExtensionBridge_dlopenLibrary
  (JNIEnv *env, jclass cls, jstring path) {
    const char *cpath = (*env)->GetStringUTFChars(env, path, NULL);
    void *handle = dlopen(cpath, RTLD_NOW);
    if (!handle) {
        fprintf(stderr, "[stratosbridge] dlopen(%s) failed: %s\n", cpath, dlerror());
    }
    (*env)->ReleaseStringUTFChars(env, path, cpath);
    return (jlong)(intptr_t)handle;
}

JNIEXPORT jlong JNICALL Java_com_stratosdb_sql_extension_NativeExtensionBridge_dlsymFunction
  (JNIEnv *env, jclass cls, jlong handle, jstring symbolName) {
    const char *csym = (*env)->GetStringUTFChars(env, symbolName, NULL);
    dlerror(); // clear any prior error, per dlsym(3)'s own documented convention for detecting a genuine failure
    void *fn = dlsym((void *)(intptr_t)handle, csym);
    const char *err = dlerror();
    if (err != NULL) {
        fprintf(stderr, "[stratosbridge] dlsym(%s) failed: %s\n", csym, err);
        fn = NULL;
    }
    (*env)->ReleaseStringUTFChars(env, symbolName, csym);
    return (jlong)(intptr_t)fn;
}

JNIEXPORT jlong JNICALL Java_com_stratosdb_sql_extension_NativeExtensionBridge_invokeIntFunction
  (JNIEnv *env, jclass cls, jlong funcPtr, jlongArray args) {
    jsize argc = (*env)->GetArrayLength(env, args);
    jlong *cargs = (*env)->GetLongArrayElements(env, args, NULL);

    stratos_ext_func_t fn = (stratos_ext_func_t)(intptr_t)funcPtr;
    int64_t result = fn((int64_t *)cargs, (int32_t)argc);

    (*env)->ReleaseLongArrayElements(env, args, cargs, JNI_ABORT);
    return (jlong)result;
}
