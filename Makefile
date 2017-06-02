UNAME_S := $(shell uname -s)

all: ramfs

ramfs: ramfs.cpp
ifeq ($(UNAME_S),Darwin)
	mpicxx ramfs.cpp -D_FILE_OFFSET_BITS=64 -lfuse_ino64 -pthread -o ramfs -Ofast -std=c++11
else
	mpicxx ramfs.cpp -D_FILE_OFFSET_BITS=64 -lfuse -pthread -o ramfs -Ofast -std=c++11
endif

clean:
	rm -f ramfs
