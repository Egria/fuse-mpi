all: ramfs

ramfs: ramfs.c
	gcc ramfs.c -D_FILE_OFFSET_BITS=64 -lfuse -pthread -o ramfs -g

clean:
	rm -f ramfs
