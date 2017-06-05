#define FUSE_USE_VERSION 26

#include <mpi.h>
#include <fuse.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <map>
#include <string>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#define NAME_MAX       128
#define PATH_MAX       1024
#define MSIZE          2
#define ROOT           0
#define PID_MAX        32768
#define BLOCK_SIZE     (1*1024 * 1024)
#define BLOCK_TABLE_GROW (1024 * 1024)
#define BLOCK_POOL_GROW  (1024)

int rank, nprocs, provided;

struct fsdata
{
    unsigned long free_bytes;
    unsigned long used_bytes;
    unsigned long total_size;
    unsigned long max_no_of_files;
    unsigned long avail_no_of_files;
};

union block
{
    uint8_t data[BLOCK_SIZE];
    union block *next;
};

union block *block_continuous_pool;
size_t block_continuous_pool_size;

union block *block_free_chain;

struct block_info
{
    union block *addr;
    int rank;
};

struct metadata
{
    unsigned long inode;
    unsigned long size;

//    char *data;  // just for dir
    block_info *blocks_table;
    size_t blocks_table_size;

    mode_t mode;
    short inuse;
    time_t accesstime;
    time_t modifiedtime;
    uid_t uid;
    gid_t gid;
};

const char *command_name[] = {"init", "getattr", "statfs", "utime", "readdir", "open", "read",
                              "create", "mkdir", "opendir", "release", "write", "rename",
                              "truncate", "unlink", "rmdir", "destroy", "chmod", "chown"
                             };
struct command
{
    enum command_type
    {
        init, getattr, statfs, utime, readdir, open, read,
        create, mkdir, opendir, release, write, rename,
        truncate, unlink, rmdir, destroy, chmod, chown
    } type;
    int source;
    char path[PATH_MAX];
    char newpath[PATH_MAX];
    uid_t uid;
    gid_t gid;
    mode_t mode;
    off_t offset;
    size_t size;
    time_t time;
};

struct request_message
{
    enum request_type
    {
        get, put
    } type;
    int source;
    long long size;
    char *base;
    bool end;
};

struct meta_message
{
    metadata meta;
    int status;
};

struct dir_message
{
    size_t size, num;
    int status;
};

fsdata fs_stat;
metadata *file;

typedef std::pair<std::string, std::string> path_pair;
typedef std::pair<path_pair, unsigned long> file_pair;
typedef std::map<path_pair, unsigned long>::iterator files_iter;
std::map<path_pair, unsigned long> files;

inline union block *alloc_block()
{
    union block *ret;
    if (block_free_chain != NULL) {
        ret = block_free_chain;
        block_free_chain = block_free_chain->next;
    } else {
        if (block_continuous_pool_size <= 0) {
            block_continuous_pool = (union block *)malloc(BLOCK_POOL_GROW *
	            sizeof(union block));
            block_continuous_pool_size = BLOCK_POOL_GROW;
        }
        ret = block_continuous_pool;
        block_continuous_pool++;
        block_continuous_pool_size--;
    }
    memset(ret, 0, sizeof(block_free_chain));
    return ret;
}

inline block_info *resize_block_table(block_info *table,
        size_t orig_size, size_t size)
{
    size_t orig_grows = orig_size / BLOCK_TABLE_GROW + 1;
    size_t required_grows = size / BLOCK_TABLE_GROW + 1;

    block_info *ret;
    if (table == NULL) {
        orig_grows = 0;
        ret = (block_info *)malloc(required_grows * BLOCK_TABLE_GROW *
                sizeof(block_info));
    } else {
        if (required_grows == orig_grows) return table;
        ret = (block_info *)realloc(table, required_grows *
                BLOCK_TABLE_GROW * sizeof(block_info));
    }

    if (required_grows > orig_grows) {
        memset(ret + orig_grows * BLOCK_TABLE_GROW,
                0, (required_grows - orig_grows) *
                BLOCK_TABLE_GROW * sizeof(block_info));
    }

    return ret;
}

void get_dirname_filename ( const char *path, char *dir_name, char *base_name )
{
    static char tmp1[PATH_MAX], tmp2[PATH_MAX];
    strcpy(tmp1, path);
    strcpy(tmp2, path);
    char *dir = dirname(tmp1);
    char *base = basename(tmp2);
    strcpy(dir_name, dir);
    strcpy(base_name, base);
}

files_iter find_file(const char *path)
{
    static char dirname[PATH_MAX], filename[NAME_MAX];
    get_dirname_filename(path, dirname, filename);
    files_iter iter = files.find(std::make_pair(dirname, filename));
    return iter;
}

void make_file(const char *dirname, const char *filename, unsigned long inode)
{
    files[std::make_pair(dirname, filename)] = inode;
}

int fill_file_data(char *dirname, char *fname, mode_t mode, uid_t uid, gid_t gid)
{
    size_t i;
    int size = (int)sizeof(file_pair);

    for ( i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        if ( file[i].inuse == 0 )
            break;
    }

    file[i].inode = i;
    file[i].size = 0;
    file[i].blocks_table = NULL;
    file[i].blocks_table_size = 0;
    file[i].inuse = 1;
    file[i].mode = S_IFREG | mode;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].uid = uid;
    file[i].gid = gid;

    make_file(dirname, fname, i);
    files_iter dir = find_file(dirname);

    file [dir->second].accesstime = time(NULL);
    file [dir->second].modifiedtime = time(NULL);

    fs_stat.free_bytes = fs_stat.free_bytes - size;
    fs_stat.used_bytes = fs_stat.used_bytes + size;
    fs_stat.avail_no_of_files--;

    return 0;
}

int fill_directory_data( char *dirname, char *fname, mode_t mode, uid_t uid, gid_t gid )
{
    size_t i;
    int file_size = (int) sizeof (file_pair);

    for ( i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        if ( file[i].inuse == 0 )
            break;
    }

    file[i].inode = i;
    file[i].size = 0;
//    file[i].data = NULL;
    file[i].inuse = 1;
    file[i].mode = S_IFDIR | mode;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].uid = uid;
    file[i].gid = gid;

    make_file(dirname, fname, i);
    files_iter dir = find_file(dirname);
    file [dir->second].accesstime = time(NULL);
    file [dir->second].modifiedtime = time(NULL);

    fs_stat.free_bytes = fs_stat.free_bytes - file_size ;
    fs_stat.used_bytes = fs_stat.used_bytes + file_size ;
    fs_stat.avail_no_of_files--;

    return 0;
}

static void server_init(uid_t uid, gid_t gid)
{
    static bool flag = false;

    if (flag) return;

    flag = true;
    unsigned long metadata_size;

    metadata_size = fs_stat.total_size * MSIZE / 100  ;
    fs_stat.max_no_of_files = metadata_size / sizeof ( metadata );
    fs_stat.avail_no_of_files = fs_stat.max_no_of_files - 1;
    fs_stat.free_bytes = fs_stat.total_size - metadata_size - sizeof(file_pair);
    fs_stat.used_bytes = sizeof ( file_pair );

    file = (metadata *) calloc ( fs_stat.max_no_of_files, sizeof ( metadata ) );

    make_file("/", "/", ROOT);

    file [ROOT].inode = 0;
    file [ROOT].size = 0;
//    file [ROOT].data = NULL;
    file [ROOT].inuse = 1;
    file [ROOT].mode = S_IFDIR | 0777;
    file [ROOT].accesstime = time(NULL);
    file [ROOT].modifiedtime = time(NULL);
    file [ROOT].uid = uid;
    file [ROOT].gid = gid;
}

static void *imfs_init(fuse_conn_info *conn)
{
    command comm;
    comm.type = comm.init, comm.source = rank;
    comm.uid = fuse_get_context()->uid, comm.gid = fuse_get_context()->gid;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    return 0;

}

static int server_getattr(const char *path)
{
    int index = 0;
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;
    else
        index = iter->second;

    return index;
}

static int imfs_getattr(const char *path, struct stat *stbuf)
{
    command comm;
    comm.type = comm.getattr, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    meta_message message;

    MPI_Recv(&message, sizeof(message), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (message.status != 0)
        return message.status;

    memset(stbuf, 0, sizeof ( struct stat ) );

    if ( S_ISDIR ( message.meta.mode ) )
    {
        stbuf->st_mode = message.meta.mode;
        stbuf->st_nlink = 2;
        stbuf->st_atime = message.meta.accesstime;
        stbuf->st_mtime = message.meta.modifiedtime;
        stbuf->st_size = 4096;
        stbuf->st_blocks = 4;
        stbuf->st_blksize = 1;
        stbuf->st_uid = message.meta.uid;
        stbuf->st_gid = message.meta.gid;
    }
    else
    {
        stbuf->st_mode = message.meta.mode;
        stbuf->st_nlink = 1;
        stbuf->st_blocks = message.meta.size;
        stbuf->st_size =  message.meta.size;
        stbuf->st_atime = message.meta.accesstime;
        stbuf->st_mtime = message.meta.modifiedtime;
        stbuf->st_blksize = 1;
        stbuf->st_uid = message.meta.uid;
        stbuf->st_gid = message.meta.gid;
    }

    return 0;
}


static int server_statfs(const char *path, struct statvfs *stbuf)
{
    memset(stbuf, 0, sizeof ( struct statvfs ) );

    stbuf->f_bsize = BLOCK_SIZE;
    stbuf->f_frsize = BLOCK_SIZE;
    stbuf->f_blocks = fs_stat.total_size/BLOCK_SIZE;
    stbuf->f_bfree = fs_stat.free_bytes/BLOCK_SIZE;
    stbuf->f_files = fs_stat.max_no_of_files;
    stbuf->f_ffree = fs_stat.avail_no_of_files;
    stbuf->f_namemax = NAME_MAX;
    stbuf->f_bavail = fs_stat.free_bytes/BLOCK_SIZE;

    return 0;
}

static int imfs_statfs(const char *path, struct statvfs *stbuf)
{
    command comm;
    comm.type = comm.statfs, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    MPI_Recv(stbuf, sizeof(struct statvfs), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return 0;
}

static int imfs_utime(const char *path, utimbuf *ubuf)
{
    command comm;
    comm.type = comm.getattr, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    meta_message message;

    MPI_Recv(&message, sizeof(message), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (message.status != 0)
        return message.status;

    ubuf->actime = message.meta.accesstime;
    ubuf->modtime = message.meta.modifiedtime;

    return 0;
}

static int server_create(const char *path, mode_t mode, uid_t uid, gid_t gid)
{
    int ret = 0;
    char dirname [PATH_MAX];
    char fname [NAME_MAX];

    if ( fs_stat.avail_no_of_files == 0 || fs_stat.free_bytes < sizeof(file_pair) )
        return -ENOSPC;

    get_dirname_filename ( path, dirname, fname );
    files_iter iter = find_file(path);

    if ( iter != files.end() )
        return -EEXIST;

    files_iter dir = find_file(dirname);

    if ( dir == files.end() || !S_ISDIR(file[dir->second].mode))
        return -ENOENT;

    ret = fill_file_data( dirname, fname, mode, uid, gid );

    return ret;
}

static int imfs_create(const char *path, mode_t mode, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.create, comm.source = rank;
    comm.uid = fuse_get_context()->uid, comm.gid = fuse_get_context()->gid;
    strcpy(comm.path, path);
    comm.mode = mode;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_mkdir(const char *path, mode_t mode, uid_t uid, gid_t gid)
{
    char dirname [PATH_MAX];
    char fname [NAME_MAX];
    int ret = 0;

    if ( fs_stat.avail_no_of_files == 0 || fs_stat.free_bytes < ( sizeof(file_pair) ) )
        return -ENOSPC;

    get_dirname_filename ( path, dirname, fname );
    files_iter iter = find_file(path);

    if ( iter != files.end() )
        return -EEXIST;

    files_iter dir = find_file(dirname);

    if ( dir == files.end() || !S_ISDIR(file[dir->second].mode))
        return -ENOENT;

    ret = fill_directory_data(dirname, fname, mode, uid, gid);

    return ret;
}

static int imfs_mkdir(const char *path, mode_t mode)
{
    command comm;
    comm.type = comm.mkdir, comm.source = rank;
    comm.uid = fuse_get_context()->uid, comm.gid = fuse_get_context()->gid;
    strcpy(comm.path, path);
    comm.mode = mode;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;

}

static int server_open(const char *path)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;

    file[iter->second].accesstime = time(NULL);

    return 0;
}

static int imfs_open(const char *path, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.open, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}


static int imfs_release(const char *path, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.release, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    return 0;
}

static int server_truncate(const char *path, off_t offset )
{
    unsigned long old_size = 0;

    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;

    old_size = file [iter->second].size;

    if ( offset == 0 )
    {
        free(file[iter->second].blocks_table);
        file[iter->second].blocks_table = NULL;
        file[iter->second].blocks_table_size = 0;
        file [iter->second].size = 0;
        fs_stat.free_bytes = fs_stat.free_bytes + old_size ;
        fs_stat.used_bytes = fs_stat.used_bytes - old_size ;
    }
    else
    {
        int block_id = (offset + 1) / BLOCK_SIZE;
        int new_size = block_id + 1;
        for (int idx = new_size;
                idx < file[iter->second].blocks_table_size; idx++) {
            file[iter->second].blocks_table[idx].addr = NULL;
        }
        file[iter->second].blocks_table = resize_block_table(
                file[iter->second].blocks_table,
                file[iter->second].blocks_table_size,
                new_size);
        file[iter->second].blocks_table_size = new_size;
        file[iter->second].size = offset + 1;
        fs_stat.free_bytes = fs_stat.free_bytes + old_size - offset + 1;
        fs_stat.used_bytes = fs_stat.used_bytes - old_size + offset + 1;
    }

    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);
    return 0;
}

static int imfs_truncate(const char *path, off_t offset )
{
    command comm;
    comm.type = comm.truncate, comm.source = rank;
    strcpy(comm.path, path);
    comm.offset = offset;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_opendir(const char *path)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;

    file[iter->second].accesstime = time(NULL);
    return 0;
}

static int imfs_opendir(const char *path, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.opendir, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_readdir(const char *path, size_t *&offset, size_t &num, char *&buf, size_t &size)
{
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    get_dirname_filename ( path, dirname, fname );
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;

    num = 2, size = 2 + 3;

    for (files_iter titer = files.lower_bound(std::make_pair(path, "")); titer != files.end() && titer->first.first == path; titer++)if (titer != iter)
        {
            num++;
            size += titer->first.second.length() + 1;
        }

    offset = new size_t[num];
    buf = new char[size];
    size_t len = 0, cur = 0;

    offset[cur++] = len;
    strcpy(buf + len, ".");
    len += 2;
    offset[cur++] = len;
    strcpy(buf + len, "..");
    len += 3;

    //filler(buf, ".", NULL, 0);
    //filler(buf, "..", NULL, 0);

    for (files_iter titer = files.lower_bound(std::make_pair(path, "")); titer != files.end() && titer->first.first == path; titer++)if (titer != iter)
        {
            offset[cur++] = len;
            strcpy(buf + len, titer->first.second.c_str());
            len += titer->first.second.length() + 1;
        }

    file [iter->second].accesstime = time(NULL);

    return 0;
}

static int imfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.readdir, comm.source = rank;
    strcpy(comm.path, path);
    comm.offset = offset;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    dir_message message;
    MPI_Recv(&message, sizeof(message), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    if (message.status < 0)
        return message.status;

    size_t *tmp_offset = new size_t[message.num];
    char *tmp_buf = new char[message.size];
    MPI_Recv(tmp_offset, message.num, MPI_OFFSET, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    MPI_Recv(tmp_buf, message.size, MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    for (size_t i = 0; i < message.num; i++)
    {
        filler(buf, tmp_buf + tmp_offset[i], NULL, 0);
    }

    delete [] tmp_offset;
    delete [] tmp_buf;

    return 0;
}

static void server_read(command &comm)
{
#define SEND_MPI MPI_Send(&request, sizeof(request), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
    request_message request;

    files_iter iter = find_file(comm.path);
    if (iter == files.end()) {
        request.end = true;
        request.size = -ENOENT;
        SEND_MPI; return;
    }

    if (S_ISDIR(file[iter->second].mode)) {
        request.end = true;
        request.size = -EISDIR;
        SEND_MPI; return;
    }

    file[iter->second].accesstime = time(NULL);

    block_info *table = file[iter->second].blocks_table;
    if (comm.offset + comm.size > file[iter->second].size)
        comm.size = file[iter->second].size - comm.offset;
    if (comm.size <= 0) {
        request.end = true;
        request.size = 0;
        SEND_MPI; return;
    }

    long long offset = comm.offset, size = comm.size;
    long long endpos = offset + size;
    long long blkno = offset / BLOCK_SIZE;          // The NO. of rd begin block
    long long nblks = endpos / BLOCK_SIZE - blkno;  // The size of rd blocks

    request.type = request.get;
    request.end = false;
    long long blkoff = offset % BLOCK_SIZE;
    if (blkoff != 0) {
        long long rs = (nblks != 0) ? (BLOCK_SIZE - blkoff) : (endpos - offset);
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data) + blkoff;
        request.size = rs;
        SEND_MPI;
        blkno++, nblks--;
    }
    if (nblks + 1 == 0) goto out;

    request.size = BLOCK_SIZE;
    while (nblks > 0) {
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data);
        SEND_MPI;
        blkno++, nblks--;
    }

    request.size = endpos % BLOCK_SIZE;
    if (request.size != 0) {
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data);
        SEND_MPI;
    }

out:
    request.end = true;
    SEND_MPI;
#undef SEND_MPI
}

static int imfs_read(const char *path, char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.read, comm.source = rank;
    strcpy(comm.path, path);
    comm.offset = offset;
    comm.size = size;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    request_message request;
    int total = 0;

    for (; ; ) {
        MPI_Recv(&request, sizeof(request), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (request.size < 0) return request.size;
        if (request.end) break;
        int target = request.source;
        request.source = rank;
        MPI_Send(&request, sizeof(request), MPI_CHAR, target, 2, MPI_COMM_WORLD);
        MPI_Recv(buf, request.size, MPI_CHAR, target, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        buf += request.size;
        total += request.size;
    }

    return total;
}

static block *server_request_block(command &comm)
{
#define SEND_MPI MPI_Send(&request, sizeof(request), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
    request_message request;
    request.end = false;
    request.size = BLOCK_SIZE;
    request.base = NULL;
    SEND_MPI;
    MPI_Recv(&request, sizeof(request), MPI_CHAR, comm.source, 10, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return (block *)request.base;
}

static void server_write(command &comm)
{
    request_message request;

    files_iter iter = find_file(comm.path);
    if (iter == files.end()) {
        request.end = true;
        request.size = -ENOENT;
        SEND_MPI; return;
    }

    if (S_ISDIR(file[iter->second].mode)) {
        request.end = true;
        request.size = -EISDIR;
        SEND_MPI; return;
    }

    long long offset = comm.offset, size = comm.size;

    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);

    long long old_size = file[iter->second].size;
    block_info *table = file[iter->second].blocks_table;

    if ((offset + size) > file[iter->second].size) {
        if (fs_stat.free_bytes < (offset + size - old_size)) {
            request.end = true;
            request.size = -ENOSPC;
            SEND_MPI; return;
        }
        long long old_block_size = file[iter->second].blocks_table_size;
        long long new_block_size = ((offset + size + 1) / BLOCK_SIZE) + 1;
        file[iter->second].blocks_table = table =
                resize_block_table(table, old_block_size, new_block_size);
        fs_stat.free_bytes = fs_stat.free_bytes + old_size - (offset + size);
        fs_stat.used_bytes = fs_stat.used_bytes - old_size + (offset + size);
        file[iter->second].size = offset + size;
    }

    long long endpos = offset + size;
    long long blkno = offset / BLOCK_SIZE;          // The NO. of rd begin block
    long long nblks = endpos / BLOCK_SIZE - blkno;  // The size of rd blocks

    request.type = request.put;
    request.end = false;
    long long blkoff = offset % BLOCK_SIZE;
    if (blkoff != 0) {
        long long rs = (nblks != 0) ? (BLOCK_SIZE - blkoff) : (endpos - offset);
        if (table[blkno].addr == NULL) {
            table[blkno].addr = server_request_block(comm);
            table[blkno].rank = comm.source;
        }
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data) + blkoff;
        request.size = rs;
        SEND_MPI;
        blkno++, nblks--;
    }
    if (nblks + 1 == 0) goto out;

    request.size = BLOCK_SIZE;
    while (nblks > 0) {
        if (table[blkno].addr == NULL) {
            table[blkno].addr = server_request_block(comm);
            table[blkno].rank = comm.source;
        }
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data);
        SEND_MPI;
        blkno++, nblks--;
    }

    request.size = endpos % BLOCK_SIZE;
    if (request.size != 0) {
        if (table[blkno].addr == NULL) {
            table[blkno].addr = server_request_block(comm);
            table[blkno].rank = comm.source;
        }
        request.source = table[blkno].rank;
        request.base = (char *)&(table[blkno].addr->data);
        SEND_MPI;
    }

out:
    request.end = true;
    SEND_MPI;
#undef SEND_MPI
}

static int imfs_write(const char *path, const char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    command comm;
    comm.type = comm.write, comm.source = rank;
    strcpy(comm.path, path);
    comm.offset = offset;
    comm.size = size;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    request_message request;
    int total = 0;

    for (; ; ) {
        MPI_Recv(&request, sizeof(request), MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (request.size < 0) return request.size;
        if (request.end) break;
        if (request.base == NULL) {
            // memory allocation
            request.base = (char *)alloc_block();
            MPI_Send(&request, sizeof(request), MPI_CHAR, 0,
                        10, MPI_COMM_WORLD);
            continue;
        }
        int target = request.source;
        request.source = rank;
        MPI_Send(&request, sizeof(request), MPI_CHAR, target, 2, MPI_COMM_WORLD);
        MPI_Send(buf, request.size, MPI_CHAR, target, 3, MPI_COMM_WORLD);
        buf += request.size;
        total += request.size;
    }

    //printf("ending write\n");
    return total;
}

static int server_chmod(const char *path, mode_t mode)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (file[iter->second].mode & S_IFDIR)
        file[iter->second].mode = S_IFDIR | mode;
    else
        file[iter->second].mode = S_IFREG | mode;

    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);

    return 0;
}
static int imfs_chmod(const char *path, mode_t mode)
{
    command comm;
    comm.type = comm.chmod, comm.source = rank;
    strcpy(comm.path, path);
    comm.mode = mode;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_chown(const char *path, uid_t uid, gid_t gid)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    file[iter->second].uid = uid;
    file[iter->second].gid = gid;

    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);

    return 0;
}

static int imfs_chown(const char *path, uid_t uid, gid_t gid)
{
    command comm;
    comm.type = comm.chown, comm.source = rank;
    strcpy(comm.path, path);
    comm.uid = uid, comm.gid = gid;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_unlink(const char *path)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;


    file [iter->second].inuse = 0;
    fs_stat.free_bytes = fs_stat.free_bytes + file [iter->second].size + sizeof ( file_pair );
    fs_stat.used_bytes = fs_stat.used_bytes - file [iter->second].size - sizeof ( file_pair );
    fs_stat.avail_no_of_files++;
    file [iter->second].size = 0;

    files.erase(iter);
    return 0;
}

static int imfs_unlink(const char *path)
{
    command comm;
    comm.type = comm.unlink, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static int server_rmdir(const char *path)
{
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;

    if ( strcmp(path, "/") == 0 )
        return -EBUSY;

    file [iter->second].inuse = 0;
//    free(file [iter->second].data);
//    file [iter->second].data = NULL;

    fs_stat.free_bytes = fs_stat.free_bytes + file [iter->second].size + sizeof ( file_pair );
    fs_stat.used_bytes = fs_stat.used_bytes - file [iter->second].size - sizeof ( file_pair );
    fs_stat.avail_no_of_files++;
    file [iter->second].size = 0;

    files.erase(iter);

    return 0;
}

static int imfs_rmdir(const char *path)
{
    command comm;
    comm.type = comm.rmdir, comm.source = rank;
    strcpy(comm.path, path);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

int server_rename(const char *path, const char *newpath)
{
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    char newdirname[PATH_MAX];
    char newfname [NAME_MAX];

    if ( strcmp(path, "/") == 0 )
        return -EBUSY;

    get_dirname_filename ( path, dirname, fname );
    get_dirname_filename ( newpath, newdirname, newfname );
    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    files_iter dir = find_file(newdirname);

    if ( dir == files.end() || !S_ISDIR(file[dir->second].mode))
        return -ENOENT;

    make_file(newdirname, newfname, iter->second);

    if (S_ISDIR(file[iter->second].mode))
    {
        size_t len = strlen(path);

        for (files_iter iter = files.lower_bound(std::make_pair(path, "")); iter != files.end() && iter->first.first.substr(0, len) == path && (iter->first.first.length() == len || iter->first.first[len] == '/'); iter++)
        {
            std::string tmp = std::string(newpath) + iter->first.first.substr(len);
            make_file(tmp.c_str(), iter->first.second.c_str(), iter->second);
            file[iter->second].accesstime = time(NULL);
            file[iter->second].modifiedtime = time(NULL);
            files_iter pre = iter;
            pre--;
            files.erase(iter);
            iter = pre;
        }
    }

    files.erase(iter);

    return 0;
}

int imfs_rename(const char *path, const char *newpath)
{
    command comm;
    comm.type = comm.rename, comm.source = rank;
    strcpy(comm.path, path);
    strcpy(comm.newpath, newpath);
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    int ret = 0;
    MPI_Recv(&ret, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    return ret;
}

static void server_destroy ()
{
    static bool flag = false;

    if (false) return ;

    flag = true;
    command comm;
    comm.type = comm.destroy, comm.source = rank;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);

    for (size_t i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        //free(file [i].data);
    }

    free(file);

    files.clear();
}

static void imfs_destroy (void *tmp)
{
    command comm;
    comm.type = comm.destroy, comm.source = rank;
    MPI_Send(&comm, sizeof(comm), MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

void server_loop()
{
    while (true)
    {
        command comm;
        MPI_Recv(&comm, sizeof(comm), MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //if(info[0] == -1 && info[1] == -1)break;
        //printf("%s, %d\n", command_name[comm.type], comm.source);
        switch (comm.type)
        {
            case command::init:
            {
                //printf("\t%d, %d\n", comm.uid, comm.gid);
                server_init(comm.uid, comm.gid);
                break;
            }

            case command::getattr:
            {
                //printf("\t%s\n", comm.path);
                int index = server_getattr(comm.path);
                meta_message message;

                if (index < 0)
                    message.status = index;
                else
                {
                    message.status = 0;
                    message.meta = file[index];
                }

                MPI_Send(&message, sizeof(message), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::statfs:
            {
                //printf("\t%s\n", comm.path);
                struct statvfs stbuf;
                server_statfs(comm.path, &stbuf);
                MPI_Send(&stbuf, sizeof(stbuf), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::create:
            {
                //printf("\t%s, %d, %d, %d\n", comm.path, comm.mode, comm.uid, comm.gid);
                int ret = server_create(comm.path, comm.mode, comm.uid, comm.gid);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::mkdir:
            {
                //printf("\t%s, %d, %d, %d\n", comm.path, comm.mode, comm.uid, comm.gid);
                int ret = server_mkdir(comm.path, comm.mode, comm.uid, comm.gid);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::open:
            {
                //printf("\t%s\n", comm.path);
                int ret = server_open(comm.path);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::truncate:
            {
                //printf("\t%s, %ld\n", comm.path, comm.offset);
                int ret = server_truncate(comm.path, comm.offset);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::opendir:
            {
                //printf("\t%s\n", comm.path);
                int ret = server_opendir(comm.path);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::readdir:
            {
                //printf("\t%s, %ld\n", comm.path, comm.offset);
                size_t num, size, *offset;
                char *buf;
                int ret = server_readdir(comm.path, offset, num, buf, size);
                dir_message message;

                if (ret < 0)
                {
                    message.status = ret;
                    MPI_Send(&message, sizeof(message), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
                }
                else
                {
                    message.status = ret;
                    message.num = num;
                    message.size = size;
                    MPI_Send(&message, sizeof(message), MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
                    MPI_Send(offset, num, MPI_OFFSET, comm.source, 1, MPI_COMM_WORLD);
                    MPI_Send(buf, size, MPI_CHAR, comm.source, 1, MPI_COMM_WORLD);
                    delete [] offset;
                    delete [] buf;
                }

                break;
            }

            case command::read:
            {
                //printf("\t%s, %ld, %ld\n", comm.path, comm.offset, comm.size);
                server_read(comm);
                break;
            }

            case command::write:
            {
                //printf("\t%s, %ld, %ld\n", comm.path, comm.offset, comm.size);
                server_write(comm);
                break;
            }

            case command::unlink:
            {
                //printf("\t%s\n", comm.path);
                int ret = server_unlink(comm.path);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::rmdir:
            {
                //printf("\t%s\n", comm.path);
                int ret = server_rmdir(comm.path);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::chmod:
            {
                //printf("\t%s, %d\n", comm.path, comm.mode);
                int ret = server_chmod(comm.path, comm.mode);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::chown:
            {
                //printf("\t%s, %d, %d\n", comm.path, comm.uid, comm.gid);
                int ret = server_chown(comm.path, comm.uid, comm.gid);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::rename:
            {
                //printf("\t%s, %s\n", comm.path, comm.newpath);
                int ret = server_rename(comm.path, comm.newpath);
                MPI_Send(&ret, 1, MPI_INT, comm.source, 1, MPI_COMM_WORLD);
                break;
            }

            case command::destroy:
                break;

            case command::release:
                //printf("\t%s\n", comm.path);
                break;

            case command::utime:
                break;
        }

        //MPI_Send(a+info[0], 1, MPI_INT, info[1], 1, MPI_COMM_WORLD);
    }
}

void monitor_loop()
{
    while (true)
    {
        request_message request;
        MPI_Recv(&request, sizeof(request), MPI_CHAR, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        //if(info[0] == -1 && info[1] == -1)break;
        if (request.type == request.get) {
            MPI_Send(request.base, request.size, MPI_CHAR, request.source, 3, MPI_COMM_WORLD);
        } else {
            MPI_Recv(request.base, request.size, MPI_CHAR, request.source, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
    }
}

int main(int argc, char *argv[])
{
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    //printf("%d %d\n", rank, nprocs);

    if (provided != MPI_THREAD_MULTIPLE)
    {
        //printf("MPI do not Support Multiple thread\n");
        exit(0);
    }

    int i = 2;

    if ( argc < 3 )
    {
        //printf("%s <mountpoint> <size in (MB)>\n", argv[0]);
        exit(-1);
    }

    unsigned long size = atol(argv[2]);

    fs_stat.total_size = size * 1024 * 1024 * nprocs; /* In bytes */

    char tmp[1024];

    while ( (i + 1) < argc )
    {
        strcpy(tmp, argv[i + 1]);
        strcpy(argv[i], tmp);
        argv[i + 1] = argv[i] + strlen(tmp) + 1;
        i++;
    }

    argc--;
    argv[argc] = NULL;

    static fuse_operations imfs_oper;

    imfs_oper.init       = imfs_init;
    imfs_oper.getattr    = imfs_getattr;
    imfs_oper.statfs     = imfs_statfs;
    imfs_oper.utime      = imfs_utime;
    imfs_oper.readdir    = imfs_readdir;
    imfs_oper.open       = imfs_open;
    imfs_oper.read       = imfs_read;
    imfs_oper.create     = imfs_create;
    imfs_oper.mkdir      = imfs_mkdir;
    imfs_oper.opendir    = imfs_opendir;
    imfs_oper.release    = imfs_release;
    imfs_oper.write      = imfs_write;
    imfs_oper.rename     = imfs_rename;
    imfs_oper.truncate   = imfs_truncate;
    imfs_oper.unlink     = imfs_unlink;
    imfs_oper.rmdir      = imfs_rmdir;
    imfs_oper.destroy    = imfs_destroy;
    imfs_oper.chmod      = imfs_chmod;
    imfs_oper.chown      = imfs_chown;
//    .access, .readlink, .mknod, .symlink,
//    .link, .chown, .release, .fsync

    std::thread monitor(monitor_loop);
    if (rank == 0) {
        server_loop();
    } else {
        fuse_main(argc, argv, &imfs_oper, NULL);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
    return 0;
}
