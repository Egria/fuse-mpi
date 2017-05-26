all: ramfs

ramfs: ramfs.cpp
	g++ ramfs.cpp -D_FILE_OFFSET_BITS=64 -lfuse -pthread -o ramfs -Ofast

clean:
	rm -f ramfs
