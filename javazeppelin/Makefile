#zeppelin java lib

#PARAM
CC      = g++
JNI_DIR = $(JAVA_HOME)/include/
JNI_MD_DIR = $(JAVA_HOME)/include/linux/
ZEPPELIN_HEAD_DIR = $(CURDIR)/../libzp/libzp/include
SLASH_PATH = $(CURDIR)/../third/slash
PINK_PATH = $(CURDIR)/../third/pink
ZEPPELIN_PATH = $(CURDIR)/../libzp/
ZEPPELIN_SRC = $(CURDIR)/../libzp/libzp/src
ZP_LIBRARY = $(CURDIR)/../libzp/libzp/lib/libzp.a 
LDFLAGS = -L/usr/local/protobuf/lib -pthread -lrt -lprotobuf
PINK_LIBRARY=$(PINK_PATH)/pink/lib/libpink.a
SLASH_LIBRARY=$(SLASH_PATH)/slash/lib/libslash.a
DEP_LIBS = $(ZP_LIBRARY) $(PINK_LIBRARY) $(SLASH_LIBRARY)

FLAGS   = -g -Wall -D__STDC_FORMAT_MACROS -I$(JNI_DIR) -I$(JNI_MD_DIR) \
					-I$(ZEPPELIN_PATH) -I $(ZEPPELIN_HEAD_DIR) -I$(SLASH_PATH) \
					-I$(ZEPPELIN_SRC) -I$(PINK_PATH) -I./include -pthread
SO_OBJS = libzeppelin.so
SOFLAGS = -shared -fPIC
C_SRC   = lib/zeppelin.cc

#.PHONY
.PHONY : clean

all: $(SO_OBJS)
	ant -buildfile zeppelin_jar/build.xml

$(SO_OBJS): $(C_SRC)
	$(CC) $(SOFLAGS) $(FLAGS) -o $@  $^ $(DEP_LIBS)  $(LDFLAGS)

clean: 
	rm $(SO_OBJS)
	rm -rf zeppelin_jar/classes
	rm -rf zeppelin_jar/doc
	rm zeppelin_jar/zeppelin.jar
