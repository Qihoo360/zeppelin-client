#!/bin/bash
CURDIR=$PWD
TMPDIR=$CURDIR/tmp$UID
PROJECT_DIR=$CURDIR/../..
PNUM=$(cat /proc/cpuinfo | grep "processor" | wc -l)

# Update submodules
cd $PROJECT_DIR && git submodule update --init
if [[ -z $? ]]; then
  echo "git submodule failed"
  exit 1
fi
cd $CURDIR

PINK_PATH=$PROJECT_DIR/third/pink
SLASH_PATH=$PROJECT_DIR/third/slash

make -C $SLASH_PATH/slash -j$PNUM
if [[ -z $? ]]; then
  echo "make slash failed"
  exit 1
fi

make -C $PINK_PATH/pink SLASH_PATH=$SLASH_PATH -j$PNUM
if [[ -z $? ]]; then
  echo "make pink failed"
  exit 1
fi

make PINK_PATH=$PINK_PATH SLASH_PATH=$SLASH_PATH -j$PNUM
if [[ -z $? ]]; then
  echo "make libzp failed"
  exit 1
fi

LIBZP_A=$CURDIR/lib/libzp.a
LIBPINK_A=$PINK_PATH/pink/lib/libpink.a
LIBSLASH_A=$SLASH_PATH/slash/lib/libslash.a

# make libzp.a with pink and slash
mkdir -p $TMPDIR && cd $TMPDIR
ar x $LIBZP_A
ar x $LIBPINK_A
ar x $LIBSLASH_A

rm -f $LIBZP_A
ar rs $LIBZP_A $TMPDIR/*.o
rm -rf $TMPDIR

echo "Done."
