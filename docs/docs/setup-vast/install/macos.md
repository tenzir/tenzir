# macOS

On macOS, you must [build VAST from source](/docs/setup-vast/build). Note that
building requires C++20 support from the compiler, so the native AppleClang
compiler can not be used.

We generally recommend the following settings:

```bash
export PATH="$(brew --prefix llvm)/bin:${PATH}"
export CC="$(brew --prefix llvm)/bin/clang"
export CXX="$(brew --prefix llvm)/bin/clang++"
export LDFLAGS="-Wl,-rpath,$(brew --prefix llvm) ${LDFLAGS}"
export CPPFLAGS="-isystem $(brew --prefix llvm)/include ${CPPFLAGS}"
export CXXFLAGS="-isystem $(brew --prefix llvm)/include/c++/v1 ${CXXFLAGS}"
```
