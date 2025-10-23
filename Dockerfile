FROM fedora:42

RUN dnf -y update && \
    dnf -y install \
      rpm-build rpmdevtools dnf-plugins-core \
      gcc gcc-c++ make cmake clang pkgconfig \
      zlib-devel bzip2-devel lz4-devel libzstd-devel snappy-devel \
      systemd-rpm-macros \
      git curl tar \
      llvm-devel clang clang-devel \
      # optional: Rust from distro (or use rustup below)
      rust cargo \
    && dnf clean all


ENV LLVM_CONFIG_PATH=/usr/bin/llvm-config \
    LIBCLANG_PATH=/usr/lib64
# Rust
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /src
