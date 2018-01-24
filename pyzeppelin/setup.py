from distutils.core import setup, Extension
setup(name = 'pyzeppelin', version = '0.0.2', ext_modules = [Extension('pyzeppelin', sources=['pyzeppelin.cc'],
     include_dirs=['../libzp', '../third/pink/', '../third/slash'],
     extra_objects=['../libzp/libzp/lib/libzp.a', '../third/pink/pink/lib/libpink.a', '../third/slash/slash/lib/libslash.a'],
     libraries=['pthread', 'protobuf'],
     extra_compile_args=['-std=c++11']
     )])
