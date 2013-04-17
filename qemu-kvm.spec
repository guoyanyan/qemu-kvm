%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
# build-time settings that support --with or --without:
#
# = kvmonly =
# Build only KVM-enabled QEMU targets, on KVM-enabled architectures.
#
# Disabled by default.
#
# = exclusive_x86_64 =
# ExclusiveArch: x86_64
#
# Disabled by default, except on RHEL.  Only makes sense with kvmonly.
#
# = rbd =
# Enable rbd support.
#
# Enable by default, except on RHEL.

%bcond_without kvmonly          # enabled
%bcond_without exclusive_x86_64 # enabled
%bcond_with    rbd              # disabled
%bcond_without spice            # enabled
%bcond_with    seccomp          # disabled

%global SLOF_gittagdate 20120731

%global kvm_archs x86_64

%ifarch %{ix86} x86_64
%if %{with seccomp}
%global have_seccomp 1
%endif
%if %{with spice}
%global have_spice   1
%endif
%endif

%global need_qemu_kvm %{with kvmonly}

# These values for system_xyz are overridden below for non-kvmonly builds.
# Instead, these values for kvm_package are overridden below for kvmonly builds.
# Somewhat confusing, but avoids complicated nested conditionals.

%ifarch x86_64
%global system_x86    kvm
%global kvm_package   system-x86
%global kvm_target    x86_64
%global need_qemu_kvm 1
%endif

%if %{with kvmonly}
# If kvmonly, put the qemu-kvm binary in the qemu-kvm package
%global kvm_package   kvm
%endif

Summary: qemu-kvm is the qemu backend for kvm
Name: qemu
Version: 1.4.0
Release: 10002
# Epoch because we pushed a qemu-1.0 package. AIUI this can't ever be dropped
Epoch: 2
License: GPLv2+ and LGPLv2+ and BSD
Group: Development/Tools
URL: http://www.qemu.org/
# RHEL will build Qemu only on x86_64:
%if %{with kvmonly}
ExclusiveArch: %{kvm_archs}
%endif

# There aren't qemu-kvm 1.2 maint releases yet, so we are carrying patches
Source0: qemu-kvm.tar.gz

BuildRequires: zlib-devel
BuildRequires: which
BuildRequires: texi2html
BuildRequires: gnutls-devel
BuildRequires: cyrus-sasl-devel
BuildRequires: libtool
BuildRequires: libaio-devel
BuildRequires: pciutils-devel
BuildRequires: ncurses-devel
BuildRequires: usbredir-devel >= 0.5.2
BuildRequires: texinfo
BuildRequires: libiscsi-devel

%if 0%{?have_spice:1}
BuildRequires: spice-protocol >= 0.12.2
BuildRequires: spice-server-devel >= 0.12.0
%endif
%if 0%{?have_seccomp:1}
BuildRequires: libseccomp-devel >= 1.0.0
%endif

%if %{with rbd}
# For rbd block driver
BuildRequires: ceph-devel
%endif

# We need both because the 'stap' binary is probed for by configure
BuildRequires: systemtap
BuildRequires: systemtap-sdt-devel

%if 0
# For network block driver
BuildRequires: libcurl-devel
# For XFS discard support in raw-posix.c
BuildRequires: xfsprogs-devel
%endif

# For VNC JPEG support
BuildRequires: libjpeg-devel
# For VNC PNG support
BuildRequires: libpng-devel
# For uuid generation
BuildRequires: libuuid-devel
%if 0%{?need_fdt:1}
# For FDT device tree support
BuildRequires: libfdt-devel
%endif
# For test suite
BuildRequires: check-devel
Requires: %{name}-img = %{epoch}:%{version}-%{release}

%define qemudocdir %{_docdir}/%{name}

%description
QEMU is a generic and open source processor emulator which achieves a good
emulation speed by using dynamic translation. QEMU has two operating modes:

 * Full system emulation. In this mode, QEMU emulates a full system (for
   example a PC), including a processor and various peripherials. It can be
   used to launch different Operating Systems without rebooting the PC or
   to debug system code.
 * User mode emulation. In this mode, QEMU can launch Linux processes compiled
   for one CPU on another CPU.

As QEMU requires no host kernel patches to run, it is safe and easy to use.

%if %{without kvmonly}
%ifarch %{kvm_archs}
%package kvm
Summary: QEMU metapackage for KVM support
Group: Development/Tools
Requires: qemu-%{kvm_package} = %{epoch}:%{version}-%{release}

%description kvm
This is a meta-package that provides a qemu-system-<arch> package for native
architectures where kvm can be enabled. For example, in an x86 system, this
will install qemu-system-x86
%endif
%endif

%package  img
Summary: QEMU command line tool for manipulating disk images
Group: Development/Tools
%if %{with rbd}
# librbd (from ceph) added new symbol rbd_flush recently.  If you
# update qemu-img without updating librdb you get:
# qemu-img: undefined symbol: rbd_flush
# ** NB ** This can be removed after Fedora 17 is released.
Conflicts: ceph < 0.37-2
%endif

%description img
This package provides a command line tool for manipulating disk images

%package  common
Summary: QEMU common files needed by all QEMU targets
Group: Development/Tools
Requires(post): /usr/bin/getent
Requires(post): /usr/sbin/groupadd
Requires(post): /usr/sbin/useradd
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units
%description common
QEMU is a generic and open source processor emulator which achieves a good
emulation speed by using dynamic translation.

This package provides the common files needed by all QEMU targets

%package qmp
Summary: QEMU QMP utilities
Group: System Environment/Daemons
Requires:       python

%description qmp
This package provides utilities to manipulate QMP.

%package guest-agent
Summary: QEMU guest agent
Group: System Environment/Daemons
Requires(post): systemd-units
Requires(preun): systemd-units
Requires(postun): systemd-units

%description guest-agent
QEMU is a generic and open source processor emulator which achieves a good
emulation speed by using dynamic translation.

This package provides an agent to run inside guests, which communicates
with the host over a virtio-serial channel named "org.qemu.guest_agent.0"

This package does not need to be installed on the host OS.

%post guest-agent
%systemd_post qemu-guest-agent.service

%preun guest-agent
%systemd_preun qemu-guest-agent.service

%postun guest-agent
%systemd_postun_with_restart qemu-guest-agent.service

%if 0%{?system_x86:1}
%package %{system_x86}
Summary: QEMU system emulator for x86
Group: Development/Tools
Requires: %{name}-common = %{epoch}:%{version}-%{release}
Provides: kvm = 85
Obsoletes: kvm < 85
Requires: vgabios >= 0.6c-2
Requires: seabios-bin >= 0.6.0-2
#Requires: sgabios-bin
Requires: ipxe-roms-qemu
Requires: libiscsi
%if 0%{?have_seccomp:1}
Requires: libseccomp >= 1.0.0
%endif

%description %{system_x86}
QEMU is a generic and open source processor emulator which achieves a good
emulation speed by using dynamic translation.

This package provides the system emulator for x86. When being run in a x86
machine that supports it, this package also provides the KVM virtualization
platform.
%endif

%prep
#%setup -q -n qemu-kvm-%{version}
%setup -q -n qemu-kvm

%build
buildarch="%{kvm_target}-softmmu"

# --build-id option is used for giving info to the debug packages.
extraldflags="-Wl,--build-id";
buildldflags="VL_LDFLAGS=-Wl,--build-id"

dobuild() {
    ./configure \
        --prefix=%{_prefix} \
        --libdir=%{_libdir} \
        --sysconfdir=%{_sysconfdir} \
        --interp-prefix=%{_prefix}/qemu-%%M \
        --audio-drv-list=oss \
        --audio-card-list=ac97,hda \
        --enable-libiscsi \
        --enable-usb-redir \
        --disable-strip \
        --disable-slirp \
        --disable-nptl \
        --disable-guest-base \
        --disable-vde \
        --disable-xfsctl \
        --disable-sdl \
        --disable-curl \
        --disable-tcg-interpreter \
        --disable-bluez \
        --disable-system \
        --disable-user \
        --disable-guest-base \
        --disable-linux-user \
        --disable-bsd-user \
        --disable-brlapi \
        --disable-attr \
        --disable-cap-ng \
        --disable-smartcard-nss \
        --disable-glusterfs \
        --disable-virtfs \
        --enable-virtio-blk-data-plane \
        --enable-spice \
        --enable-mixemu \
        --disable-rbd \
        --disable-fdt \
        --enable-trace-backend=dtrace \
        --disable-werror \
        --disable-xen \
        --enable-kvm \
        --extra-ldflags="$extraldflags -pie -Wl,-z,relro -Wl,-z,now" \
        --extra-cflags="%{optflags} -fPIE -DPIE" \
        "$@"

    echo "config-host.mak contents:"
    echo "==="
    cat config-host.mak
    echo "==="

    make V=1 %{?_smp_mflags} $buildldflags
}

# This is kind of confusing. We run ./configure + make twice here to
# preserve some back compat: if on x86, we want to provide a qemu-kvm
# binary that defaults to KVM=on. All other qemu-system* should be
# able to use KVM, but default to KVM=off (upstream qemu semantics).
#
# Once qemu-kvm and qemu fully merge, and we base off qemu releases,
# all qemu-system-* will default to KVM=off, so we hopefully won't need
# to do these double builds. But then I'm not sure how we are going to
# generate a back compat qemu-kvm binary...

%if 0%{?need_qemu_kvm}
# Build qemu-kvm back compat binary
dobuild --target-list=%{kvm_target}-softmmu

# Setup back compat qemu-kvm binary which defaults to KVM=on
./scripts/tracetool.py --backend dtrace --format stap \
  --binary %{_bindir}/qemu-kvm --target-arch %{kvm_target} --target-type system \
  --probe-prefix qemu.kvm < ./trace-events > qemu-kvm.stp

cp -a %{kvm_target}-softmmu/qemu-system-%{kvm_target} qemu-kvm

%endif

%if %{without kvmonly}
%if 0%{?need_qemu_kvm}
make clean
%endif

# Build qemu-system-* with consistent default of kvm=off
dobuild --target-list="$buildarch" 
%endif


%install

%define _udevdir /lib/udev/rules.d

%ifarch %{kvm_archs}
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/modules
mkdir -p $RPM_BUILD_ROOT%{_bindir}/
mkdir -p $RPM_BUILD_ROOT%{_udevdir}
mkdir -p $RPM_BUILD_ROOT%{python_sitelib}/

install -m 0755 kvm.modules $RPM_BUILD_ROOT%{_sysconfdir}/sysconfig/modules/kvm.modules
install -m 0755 scripts/kvm/kvm_stat $RPM_BUILD_ROOT%{_bindir}/
install -m 0644 80-kvm.rules $RPM_BUILD_ROOT%{_udevdir}
%endif

make DESTDIR=$RPM_BUILD_ROOT install

install -m 0755 QMP/qmp $RPM_BUILD_ROOT%{_bindir}/
install -m 0755 QMP/qmp-shell $RPM_BUILD_ROOT%{_bindir}/
install -m 0755 QMP/qom-get $RPM_BUILD_ROOT%{_bindir}/
install -m 0755 QMP/qom-set $RPM_BUILD_ROOT%{_bindir}/
install -m 0755 QMP/qom-list $RPM_BUILD_ROOT%{_bindir}/
install -m 0644 QMP/qmp.py $RPM_BUILD_ROOT%{python_sitelib}/


%if 0%{?need_qemu_kvm}
mkdir -p $RPM_BUILD_ROOT%{_datadir}/%{name}
mkdir -p $RPM_BUILD_ROOT%{_datadir}/systemtap/tapset

install -m 0755 qemu-kvm $RPM_BUILD_ROOT%{_bindir}/
install -m 0644 qemu-kvm.stp $RPM_BUILD_ROOT%{_datadir}/systemtap/tapset/
%endif

%if %{with kvmonly}
rm $RPM_BUILD_ROOT%{_bindir}/qemu-system-%{kvm_target}
rm $RPM_BUILD_ROOT%{_datadir}/systemtap/tapset/qemu-system-%{kvm_target}.stp
%endif

chmod -x ${RPM_BUILD_ROOT}%{_mandir}/man1/*
install -D -p -m 0644 -t ${RPM_BUILD_ROOT}%{qemudocdir} Changelog README COPYING COPYING.LIB LICENSE

install -D -p -m 0644 qemu.sasl $RPM_BUILD_ROOT%{_sysconfdir}/sasl2/qemu.conf

# Provided by package openbios
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/openbios-ppc
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/openbios-sparc32
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/openbios-sparc64
# Provided by package SLOF
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/slof.bin

# Remove possibly unpackaged files.  Unlike others that are removed
# unconditionally, these firmware files are still distributed as a binary
# together with the qemu package.  We should try to move at least s390-zipl.rom
# to a separate package...  Discussed here on the packaging list:
# https://lists.fedoraproject.org/pipermail/packaging/2012-July/008563.html
%if 0%{!?system_alpha:1}
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/palcode-clipper
%endif
%if 0%{!?system_microblaze:1}
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/petalogix*.dtb
%endif
%if 0%{!?system_ppc:1}
rm -f ${RPM_BUILD_ROOT}%{_datadir}/%{name}/bamboo.dtb
rm -f ${RPM_BUILD_ROOT}%{_datadir}/%{name}/ppc_rom.bin
rm -f ${RPM_BUILD_ROOT}%{_datadir}/%{name}/spapr-rtas.bin
%endif
%if 0%{!?system_s390x:1}
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/s390-zipl.rom
%endif

# Provided by package ipxe
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/pxe*rom
# Provided by package vgabios
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/vgabios*bin
# Provided by package seabios
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/bios.bin
# Provided by package sgabios
rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/sgabios.bin

# remove all roms
#rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/*.rom
#rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/*.bin
#rm -rf ${RPM_BUILD_ROOT}%{_datadir}/%{name}/*.aml

%if 0%{?system_x86:1}
# the pxe gpxe images will be symlinks to the images on
# /usr/share/ipxe, as QEMU doesn't know how to look
# for other paths, yet.
pxe_link() {
  ln -s ../ipxe/$2.rom %{buildroot}%{_datadir}/%{name}/pxe-$1.rom
}

pxe_link e1000 8086100e
#pxe_link ne2k_pci 10ec8029
#pxe_link pcnet 10222000
#pxe_link rtl8139 10ec8139
pxe_link virtio 1af41000

rom_link() {
    ln -s $1 %{buildroot}%{_datadir}/%{name}/$2
}

rom_link ../vgabios/VGABIOS-lgpl-latest.bin vgabios.bin
rom_link ../vgabios/VGABIOS-lgpl-latest.cirrus.bin vgabios-cirrus.bin
rom_link ../vgabios/VGABIOS-lgpl-latest.qxl.bin vgabios-qxl.bin
rom_link ../vgabios/VGABIOS-lgpl-latest.stdvga.bin vgabios-stdvga.bin
rom_link ../seabios/bios.bin bios.bin
#rom_link ../vgabios/VGABIOS-lgpl-latest.vmware.bin vgabios-vmware.bin
#rom_link ../sgabios/sgabios.bin sgabios.bin
%endif

# for efi binaries
install -m 0644 pc-bios/efi-e1000.rom    $RPM_BUILD_ROOT%{_datadir}/%{name}
install -m 0644 pc-bios/efi-virtio.rom   $RPM_BUILD_ROOT%{_datadir}/%{name}
#install -m 0644 pc-bios/efi-pcnet.rom    $RPM_BUILD_ROOT%{_datadir}/%{name}
#install -m 0644 pc-bios/efi-rtl8139.rom  $RPM_BUILD_ROOT%{_datadir}/%{name}
#install -m 0644 pc-bios/efi-ne2k_pci.rom $RPM_BUILD_ROOT%{_datadir}/%{name}


# For the qemu-guest-agent subpackage install the systemd
# service and udev rules.
mkdir -p $RPM_BUILD_ROOT%{_unitdir}
mkdir -p $RPM_BUILD_ROOT%{_udevdir}
install -m 0644 qemu-guest-agent.service $RPM_BUILD_ROOT%{_unitdir}
install -m 0644 99-qemu-guest-agent.rules $RPM_BUILD_ROOT%{_udevdir}

# Install rules to use the bridge helper with libvirt's virbr0
install -m 0644 bridge.conf $RPM_BUILD_ROOT%{_sysconfdir}/qemu
chmod u+s $RPM_BUILD_ROOT%{_libexecdir}/qemu-bridge-helper

find $RPM_BUILD_ROOT -name vscclient | xargs rm -f

%check
make check

%ifarch %{kvm_archs}
%post %{kvm_package}
# load kvm modules now, so we can make sure no reboot is needed.
# If there's already a kvm module installed, we don't mess with it
sh %{_sysconfdir}/sysconfig/modules/kvm.modules || :
udevadm trigger --sysname-match=kvm || :
%endif


%post common
getent group kvm >/dev/null || groupadd -g 36 -r kvm
getent group qemu >/dev/null || groupadd -g 107 -r qemu
getent passwd qemu >/dev/null || \
  useradd -r -u 107 -g qemu -G kvm -d / -s /sbin/nologin \
    -c "qemu user" qemu

%preun common

%postun common

%global kvm_files \
%{_sysconfdir}/sysconfig/modules/kvm.modules \
%{_udevdir}/80-kvm.rules

%if 0%{?need_qemu_kvm}
%global qemu_kvm_files \
%{_bindir}/qemu-kvm \
%{_datadir}/systemtap/tapset/qemu-kvm.stp
%endif

%files
%defattr(-,root,root)

%ifarch %{kvm_archs}
%files kvm
%defattr(-,root,root)
%endif

%files common
%defattr(-,root,root)
%dir %{qemudocdir}
%doc %{qemudocdir}/Changelog
%doc %{qemudocdir}/README
%doc %{qemudocdir}/qemu-doc.html
%doc %{qemudocdir}/qemu-tech.html
%doc %{qemudocdir}/COPYING
%doc %{qemudocdir}/COPYING.LIB
%doc %{qemudocdir}/LICENSE
%doc %{qemudocdir}/qmp-commands.txt
%dir %{_datadir}/%{name}/
%{_datadir}/%{name}/keymaps/
%{_mandir}/man1/qemu.1*
#%{_mandir}/man1/virtfs-proxy-helper.1*
%{_bindir}/kvm_stat
#%{_bindir}/virtfs-proxy-helper
%{_libexecdir}/qemu-bridge-helper
%config(noreplace) %{_sysconfdir}/sasl2/qemu.conf
%dir %{_sysconfdir}/qemu
%config(noreplace) %{_sysconfdir}/qemu/bridge.conf

%files guest-agent
%defattr(-,root,root,-)
%doc COPYING README
%{_bindir}/qemu-ga
%{_unitdir}/qemu-guest-agent.service
%{_udevdir}/99-qemu-guest-agent.rules

%if 0%{?system_x86:1}
%files %{system_x86}
%defattr(-,root,root)
%if %{without kvmonly}
%{_bindir}/qemu-system-i386
%{_bindir}/qemu-system-x86_64
%{_datadir}/systemtap/tapset/qemu-system-i386.stp
%{_datadir}/systemtap/tapset/qemu-system-x86_64.stp
%endif
%{_datadir}/%{name}/acpi-dsdt.aml
%{_datadir}/%{name}/q35-acpi-dsdt.aml
%{_datadir}/%{name}/bios.bin
#%{_datadir}/%{name}/sgabios.bin
%{_datadir}/%{name}/linuxboot.bin
%{_datadir}/%{name}/multiboot.bin
%{_datadir}/%{name}/kvmvapic.bin
%{_datadir}/%{name}/vgabios.bin
%{_datadir}/%{name}/vgabios-cirrus.bin
%{_datadir}/%{name}/vgabios-qxl.bin
%{_datadir}/%{name}/vgabios-stdvga.bin
#%{_datadir}/%{name}/vgabios-vmware.bin
%{_datadir}/%{name}/pxe-e1000.rom
%{_datadir}/%{name}/pxe-virtio.rom
#%{_datadir}/%{name}/pxe-pcnet.rom
#%{_datadir}/%{name}/pxe-rtl8139.rom
#%{_datadir}/%{name}/pxe-ne2k_pci.rom
%{_datadir}/%{name}/efi-e1000.rom
%{_datadir}/%{name}/efi-virtio.rom
#%{_datadir}/%{name}/efi-pcnet.rom
#%{_datadir}/%{name}/efi-rtl8139.rom
#%{_datadir}/%{name}/efi-ne2k_pci.rom
%{_datadir}/%{name}/qemu-icon.bmp
%config(noreplace) %{_sysconfdir}/qemu/target-x86_64.conf
%ifarch %{ix86} x86_64
%{?kvm_files:}
%{?qemu_kvm_files:}
%endif
%endif


%files img
%defattr(-,root,root)
%{_bindir}/qemu-img
%{_bindir}/qemu-io
%{_bindir}/qemu-nbd
%{_mandir}/man1/qemu-img.1*
%{_mandir}/man8/qemu-nbd.8*

%files qmp
%defattr(-,root,root)
%{python_sitelib}/qmp.py*
%{_bindir}/qmp
%{_bindir}/qmp-shell
%{_bindir}/qom-get
%{_bindir}/qom-list
%{_bindir}/qom-set

%changelog
* Sat Apr 06 2013 Hao Luo <hluo@litevirt.com> - 2:1.4.0-10002
- Remove pcbios dependencies

* Sat Apr 06 2013 Hao Luo <hluo@litevirt.com> - 2:1.4.0-10001
- Packaging qmp utilies

* Sat Oct 14 2012 Hao Luo <hluo@litevirt.com> - 2:1.4.0-10000
- Initiate qemu-kvm rpm package against litevirt
