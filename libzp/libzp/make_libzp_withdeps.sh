#!/bin/bash
CURDIR=$PWD
TMPDIR=$CURDIR/tmp$UID
PROJECT_DIR=$CURDIR/../..

# Update submodules
cd $PROJECT_DIR && git submodule update --init
if [[ -z $? ]]; then
  echo "git submodule failed"
  exit 1
fi
cd $CURDIR

PINK_PATH=$PROJECT_DIR/third/pink
SLASH_PATH=$PROJECT_DIR/third/slash

make -C $SLASH_PATH/slash
if [[ -z $? ]]; then
  echo "make slash failed"
  exit 1
fi

make -C $PINK_PATH/pink SLASH_PATH=$SLASH_PATH
if [[ -z $? ]]; then
  echo "make pink failed"
  exit 1
fi

make PINK_PATH=$PINK_PATH SLASH_PATH=$SLASH_PATH
if [[ -z $? ]]; then
  echo "make libzp failed"
  exit 1
fi

LIBZP_A=$CURDIR/lib/libzp.a
LIBZP_SO=$CURDIR/lib/libzp.so
LIBPINK_A=$PINK_PATH/pink/lib/libpink.a
LIBSLASH_A=$SLASH_PATH/slash/lib/libslash.a

# make libzp.a with pink and slash
mkdir -p $TMPDIR && cd $TMPDIR
ar x $LIBZP_A
ar x $LIBPINK_A
ar x $LIBSLASH_A

rm -f $LIBZP_A $LIBZP_SO
ar rs $LIBZP_A $TMPDIR/*.o
g++ -o $LIBZP_SO -shared -fPIC $TMPDIR/*.o
rm -rf $TMPDIR

# make dist
rm -rf $CURDIR/output
mkdir $CURDIR/output
mkdir -p $CURDIR/output/include/pink
mkdir -p $CURDIR/output/include/slash
cp -r $CURDIR/lib $CURDIR/output
cp -r $PINK_PATH/pink/include $CURDIR/output/include/pink
cp -r $SLASH_PATH/slash/include $CURDIR/output/include/slash

echo "Done."
