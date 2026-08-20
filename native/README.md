# StratosDB native extensions

This directory holds the source for StratosDB's own **native extension bridge** -
the mechanism behind `CREATE EXTENSION` and `CREATE FUNCTION ... LANGUAGE C`.

## What this actually is

Real Postgres extensions are C shared libraries, loaded via `dlopen`, with SQL-callable
functions resolved by symbol name via `dlsym` and Postgres's own function manager
(`fmgr`). StratosDB is written in Java, not C, so it can't use `fmgr` - but the same
underlying mechanism (`dlopen`/`dlsym`, loading and calling native code at runtime) is
exactly what this bridge gives it, reached from Java through JNI.

**The real design problem this bridge solves**: JNI's own convention binds a native
method to one fixed, compile-time-known C symbol name
(`Java_com_stratosdb_..._methodName`). If every extension's own `.so` file tried to
export that same symbol to be loaded directly via `System.load()`, the second extension
to load would collide with or silently shadow the first. So StratosDB loads only **one**
native library through JNI's own mechanism, ever - this bridge - and that bridge uses
real, ordinary `dlopen()`/`dlsym()` internally to load and call *your* extension's own
`.so` file, by name, at runtime. This is the same real approach real Postgres's own C
extensions use, just reached one layer differently.

## Do I need to build this?

Only if you actually want to use `CREATE EXTENSION`. StratosDB runs completely normally
without ever building or touching anything in this directory.

## Building it

```
./native/build.sh
```

Requires `gcc` and a JDK install (its own `jni.h`/`jni_md.h` headers are all that's
needed - no other dependency). The script auto-detects `JAVA_HOME` from a few common
locations if it isn't already set.

This produces `native/libstratosbridge.so`. StratosDB looks for it in `./native/` (relative
to wherever you start it) or `./`, or in the directory named by the `STRATOSDB_NATIVE_DIR`
environment variable if you'd rather keep it somewhere else.

## Writing your own extension

An extension is any shared library exporting one or more functions with this exact C
signature:

```c
#include <stdint.h>
int64_t your_function_name(int64_t *args, int32_t argc) {
    // args[0], args[1], ... - argc of them
    return /* your int64 result */;
}
```

Compile it normally: `gcc -shared -fPIC your_extension.c -o libyourextension.so`.

Then, from SQL:

```sql
CREATE EXTENSION myext AS '/path/to/libyourextension.so';
CREATE FUNCTION my_func(a INT, b INT) RETURNS INT AS myext, 'your_function_name' LANGUAGE C;
SELECT my_func(3, 4);
```

`CREATE EXTENSION` calls `dlopen` immediately and fails right away with a clear error if
the path is wrong or the library won't load. `CREATE FUNCTION ... LANGUAGE C` calls
`dlsym` immediately too, and fails right away if the symbol doesn't resolve - neither
error is deferred to the function's first actual call.

## Real, honestly-stated limitations

- **Integer-only.** Every argument and the return value must be a 64-bit integer. No
  strings, floats, booleans, or any other SQL type can cross this boundary yet - this is
  the bridge's own real, current calling convention, not a simplification for the
  example above.
- **A loaded library can never be unloaded.** This is a real, well-known property of
  `dlopen` inside a long-running process, not something this bridge works around.
  `DROP EXTENSION` removes StratosDB's own SQL-level registration - so the extension's
  functions stop being callable - but the shared library itself stays mapped into the
  process until the JVM itself exits.
- **No sandboxing whatsoever.** A loaded extension's native code runs with the exact
  same privileges as the StratosDB process. A memory-unsafe or malicious extension can
  corrupt memory or crash the entire database process, or do anything else native code
  can do on the machine it's running on. Treat `CREATE EXTENSION` the same way you'd
  treat running arbitrary native code - because that is exactly what it does. Real
  Postgres's own C extensions carry this same risk.
- **Not part of the Maven build.** Maven doesn't compile C code; this is a real,
  separate, manual step. A CI pipeline or packaging process that wants extensions
  available out of the box needs to run `native/build.sh` (or an equivalent) itself.
