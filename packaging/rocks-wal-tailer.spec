Name:           rocks-wal-tailer
Version:        0.1.0
Release:        1%{?dist}
Summary:        ClickHouse EmbeddedRocksDB WAL tailer to ClickHouse

License:        MIT
URL:            https://example.com/rocks-wal-tailer
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  rust cargo
BuildRequires:  gcc gcc-c++ make cmake pkgconfig clang
BuildRequires:  zlib-devel bzip2-devel lz4-devel libzstd-devel snappy-devel
BuildRequires:  systemd
BuildRequires:  systemd-rpm-macros
Requires:       systemd
Requires:       clickhouse-server
Requires:       /usr/bin/hostname
Requires(post):   systemd
Requires(preun):  systemd
Requires(postun): systemd

%description
Tails ClickHouse EmbeddedRocksDB write-ahead-logs and streams changes into a
ClickHouse changelog table. Ships with a systemd service unit.

%prep
%setup -q

%build
cargo build --release --locked

%install
install -D -m0755 target/release/rocks-wal-tailer \
  %{buildroot}%{_bindir}/rocks-wal-tailer

install -D -m0644 packaging/systemd/rocks-wal-tailer.service \
  %{buildroot}%{_unitdir}/rocks-wal-tailer.service

install -D -m0644 packaging/sysconfig/rocks-wal-tailer \
  %{buildroot}%{_sysconfdir}/sysconfig/rocks-wal-tailer

install -D -m0644 packaging/tmpfiles.d/rocks-wal-tailer.conf \
  %{buildroot}%{_tmpfilesdir}/rocks-wal-tailer.conf

%pre
exit 0

%post
%tmpfiles_create %{_tmpfilesdir}/rocks-wal-tailer.conf
%systemd_post rocks-wal-tailer.service

%preun
%systemd_preun rocks-wal-tailer.service

%postun
%systemd_postun_with_restart rocks-wal-tailer.service

%files
%{_bindir}/rocks-wal-tailer
%{_unitdir}/rocks-wal-tailer.service
%config(noreplace) %{_sysconfdir}/sysconfig/rocks-wal-tailer
%{_tmpfilesdir}/rocks-wal-tailer.conf
