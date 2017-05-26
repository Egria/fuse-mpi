/*********************************************************
 *
 *        Program implementing an in-memory filesystem
 *            (ie, RAMDISK) using FUSE.
 *
 *********************************************************/

/*................. Include Files .......................*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <map>
#include <string>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <libgen.h>
#include <sys/types.h>
#include <sys/stat.h>
#define NAME_MAX       4096
#define PATH_MAX       4096*2
#define MSIZE          2
#define  ROOT          0


/*............. Global Declarations .................*/


/* File system Information */

struct fsdata
{

    unsigned long free_bytes;
    unsigned long used_bytes;
    unsigned long total_size;
    unsigned long max_no_of_files;
    unsigned long avail_no_of_files;
};


/* File information */

struct metadata
{

    unsigned long inode;
    unsigned long size;
    char *data;
    mode_t mode;
    short inuse;
    time_t accesstime;
    time_t modifiedtime;
    uid_t uid;
    gid_t gid;
};



/* File information maintained by a directory */

struct list
{

    char fname [NAME_MAX];
    unsigned long inode;
    struct list *next;
};


/* Directory information */

fsdata fs_stat;
metadata *file;

typedef std::pair<std::string, std::string> path_pair;
typedef std::pair<path_pair, unsigned long> file_pair;
typedef std::map<path_pair, unsigned long>::iterator files_iter;
std::map<path_pair, unsigned long> files;

/*
 ** Function to get directory path and filename relative to the directory.
 */

void get_dirname_filename ( const char *path, char *dir_name, char *base_name )
{
    static char tmp1[PATH_MAX], tmp2[NAME_MAX];
    strcpy(tmp1, path);
    strcpy(tmp2, path);
    char *dir = dirname(tmp1);
    char *base = basename(tmp2);
    strcpy(dir_name, dir);
    strcpy(base_name, base);
}

files_iter find_file(const char * path)
{
    static char dirname[PATH_MAX], filename[NAME_MAX];
    get_dirname_filename(path, dirname, filename);
    files_iter iter = files.find(std::make_pair(dirname, filename));
    return iter;
}

void make_file(const char * dirname, const char * filename, unsigned long inode)
{
    files[std::make_pair(dirname, filename)] = inode;
}
/*
 ** Fill the metadata information for the file when created
 ** Accordingly add an entry into the directory structure.
 */

int fill_file_data(char *dirname, char *fname, mode_t mode)
{
    int i;
    int size = (int)sizeof(file_pair);

    for ( i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        if ( file[i].inuse == 0 )
            break;
    }

    /* Fill the metadata info */

    file[i].inode = i;
    file[i].size = 0;
    file[i].data = NULL;
    file[i].inuse = 1;
//    file[i].mode = S_IFREG | 0777;
    file[i].mode = S_IFREG | mode;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].uid = fuse_get_context()->uid;
    file[i].gid = fuse_get_context()->gid;


    /* Add an entry into directory */

    make_file(dirname, fname, i);
    files_iter dir = find_file(dirname);
    
    file [dir->second].accesstime = time(NULL);
    file [dir->second].modifiedtime = time(NULL);

    fs_stat.free_bytes = fs_stat.free_bytes - size;
    fs_stat.used_bytes = fs_stat.used_bytes + size;
    fs_stat.avail_no_of_files--;

    return 0;
}


/*
 ** Fill the metadata information for the directory when created
 ** Add a directory entry.
 */

int fill_directory_data( char *dirname, char *fname, mode_t mode )
{
    int i;
    int file_size = (int) sizeof (file_pair);

    for ( i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        if ( file[i].inuse == 0 )
            break;
    }

    /* Fill the metadata info */

    file[i].inode = i;
    file[i].size = 0;
    file[i].data = NULL;
    file[i].inuse = 1;
//    file[i].mode = S_IFDIR | 0777;
    file[i].mode = S_IFDIR | mode;
    file[i].accesstime = time(NULL);
    file[i].modifiedtime = time(NULL);
    file[i].uid = fuse_get_context()->uid;
    file[i].gid = fuse_get_context()->gid;

    /* Allocate and Populate the directory structure */
    make_file(dirname, fname, i);
    files_iter dir = find_file(dirname);
    file [dir->second].accesstime = time(NULL);
    file [dir->second].modifiedtime = time(NULL);

    fs_stat.free_bytes = fs_stat.free_bytes - file_size ;
    fs_stat.used_bytes = fs_stat.used_bytes + file_size ;
    fs_stat.avail_no_of_files--;

    return 0;

}



static void *imfs_init(fuse_conn_info *conn)
{

    unsigned long metadata_size;

    /*---------------------------------------------------------
       Initialize the File System structure.

      Metadata size will be MSIZE percent of total size of FS
    ----------------------------------------------------------*/

    metadata_size = fs_stat.total_size * MSIZE / 100  ;
    fs_stat.max_no_of_files = metadata_size / sizeof ( metadata );
    fs_stat.avail_no_of_files = fs_stat.max_no_of_files - 1;
    fs_stat.free_bytes = fs_stat.total_size - metadata_size - sizeof(file_pair);
    fs_stat.used_bytes = sizeof ( file_pair );

    file = (metadata *) calloc ( fs_stat.max_no_of_files, sizeof ( metadata ) );

    make_file("/", "/", ROOT);

    file [ROOT].inode = 0;
    file [ROOT].size = 0;
    file [ROOT].data = NULL;
    file [ROOT].inuse = 1;
    file [ROOT].mode = S_IFDIR | 0777;
    file [ROOT].accesstime = time(NULL);
    file [ROOT].modifiedtime = time(NULL);
    file [ROOT].uid = fuse_get_context()->uid;
    file [ROOT].gid = fuse_get_context()->gid;

    return 0;

}

static int imfs_getattr(const char *path, struct stat *stbuf)
{

    int index = 0;

    memset(stbuf, 0, sizeof ( struct stat ) );
    files_iter iter = find_file(path);


    if ( iter == files.end() )
        return -ENOENT;
    else
        index = iter->second;

    if ( S_ISDIR ( file [index].mode ) )
    {
        stbuf->st_mode = file [index].mode;
        stbuf->st_nlink = 2;
        stbuf->st_atime = file [index].accesstime;
        stbuf->st_mtime = file [index].modifiedtime;
        stbuf->st_size = 4096;
        stbuf->st_blocks = 4;
        stbuf->st_blksize = 1;
        stbuf->st_uid = file [index].uid;
        stbuf->st_gid = file [index].gid;
    }
    else
    {
        stbuf->st_mode = file [index].mode;
        stbuf->st_nlink = 1;
        stbuf->st_blocks = file [index].size;
        stbuf->st_size =  file [index].size;
        stbuf->st_atime = file [index].accesstime;
        stbuf->st_mtime = file [index].modifiedtime;
        stbuf->st_blksize = 1;
        stbuf->st_uid = file [index].uid;
        stbuf->st_gid = file [index].gid;
    }

    return 0;
}


static int imfs_statfs(const char *path, struct statvfs *stbuf)
{

    int res;

    memset(stbuf, 0, sizeof ( struct statvfs ) );

    stbuf->f_bsize = 4096;
    stbuf->f_frsize = 4096;
    stbuf->f_blocks = fs_stat.total_size;
    stbuf->f_bfree = fs_stat.free_bytes;
    stbuf->f_files = fs_stat.max_no_of_files;
    stbuf->f_ffree = fs_stat.avail_no_of_files;
    stbuf->f_namemax = NAME_MAX;
    stbuf->f_bavail = fs_stat.free_bytes;

    return 0;
}


int imfs_utime(const char *path, utimbuf *ubuf)
{

    int ret = 0;
    char dirname[PATH_MAX];
    char fname [NAME_MAX];

    files_iter iter = find_file(path);

    if ( iter == files.end() )
        return -ENOENT;

    ubuf->actime = file [iter->second].accesstime;
    ubuf->modtime = file [iter->second].modifiedtime;

    return ret;
}

/* Create a regular file */

static int imfs_create(const char *path, mode_t mode, fuse_file_info *fi)
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

    ret = fill_file_data( dirname, fname, mode );

    return ret;
}


/* Create a directory */

static int imfs_mkdir(const char *path, mode_t mode)
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
    
    ret = fill_directory_data(dirname, fname, mode);

    return ret;

}


static int imfs_open(const char *path, fuse_file_info *fi)
{
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;
    file[iter->second].accesstime = time(NULL);

    return 0;
}


static int imfs_release(const char *path, fuse_file_info *fi)
{
    return 0;

}

static int imfs_truncate(const char *path, off_t offset )
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
        free(file [iter->second].data);
        file [iter->second].data = NULL;
        file [iter->second].size = 0;
        fs_stat.free_bytes = fs_stat.free_bytes + old_size ;
        fs_stat.used_bytes = fs_stat.used_bytes - old_size ;
    }
    else
    {

        file [iter->second].data = (char *) realloc( file[iter->second].data, offset + 1);
        file [iter->second].size = offset + 1;
        fs_stat.free_bytes = fs_stat.free_bytes + old_size - offset + 1;
        fs_stat.used_bytes = fs_stat.used_bytes - old_size + offset + 1;
    }

    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);
    return 0;
}



static int imfs_opendir(const char *path, fuse_file_info *fi)
{
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;

    file[iter->second].accesstime = time(NULL);
    return 0;
}



static int imfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, fuse_file_info *fi)
{
    char dirname[PATH_MAX];
    char fname [NAME_MAX];
    get_dirname_filename ( path, dirname, fname );
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    
    for(files_iter titer = files.lower_bound(std::make_pair(path, "")); titer != files.end() && titer->first.first == path; titer++)if(titer != iter)
    {
        filler(buf, titer->first.second.c_str(), NULL, 0);
    }
    file [iter->second].accesstime = time(NULL);

    return 0;
}

static int imfs_read(const char *path, char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;

    if ( file[iter->second].data != NULL  &&  ( offset < file[iter->second].size ) )
    {
        if (offset + size > file[iter->second].size )
            size = file[iter->second].size - offset;

        memcpy( buf, file[iter->second].data + offset, size );
    }
    else
        size = 0;
    file[iter->second].accesstime = time(NULL);

    return size;
}

int imfs_write(const char *path, const char *buf, size_t size, off_t offset, fuse_file_info *fi)
{
    unsigned long old_size = 0;

    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;

    if ( file [iter->second].data == NULL )
    {
        if ( fs_stat.free_bytes < size )
            return -ENOSPC;

        file [iter->second].data = (char *) malloc( offset + size);

        if ( file [iter->second].data == NULL )
        {
            perror("malloc:");
            return -ENOMEM;
        }

        memset(file [iter->second].data, 0, offset + size);
        file [iter->second].size = offset + size;
        fs_stat.free_bytes = fs_stat.free_bytes - (offset + size);
        fs_stat.used_bytes = fs_stat.used_bytes + offset + size;
    }
    else
    {

        old_size = file [iter->second].size;

        if ( (offset + size) > file[iter->second].size )
        {
            if ( fs_stat.free_bytes < ( offset + size - old_size ) )
                return -ENOSPC;

            file [iter->second].data = (char *) realloc( file[iter->second].data, (offset + size) );
            fs_stat.free_bytes = fs_stat.free_bytes + old_size - ( offset + size );
            fs_stat.used_bytes = fs_stat.used_bytes - old_size + ( offset + size );
            file [iter->second].size = offset + size;
        }
    }

    memcpy(file[iter->second].data + offset, buf, size);
    file[iter->second].accesstime = time(NULL);
    file[iter->second].modifiedtime = time(NULL);
    return size;
}

static int imfs_chmod(const char *path, mode_t mode)
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

static int imfs_chown(const char *path, uid_t uid, gid_t gid)
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

static int imfs_unlink(const char *path)
{
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (S_ISDIR(file[iter->second].mode))
        return -EISDIR;


    file [iter->second].inuse = 0;
    free(file [iter->second].data);
    file [iter->second].data = NULL;

    fs_stat.free_bytes = fs_stat.free_bytes + file [iter->second].size + sizeof ( file_pair );
    fs_stat.used_bytes = fs_stat.used_bytes - file [iter->second].size - sizeof ( file_pair );
    fs_stat.avail_no_of_files++;
    file [iter->second].size = 0;

    files.erase(iter);
    return 0;
}


static int imfs_rmdir(const char *path)
{
    files_iter iter = find_file(path);
    if ( iter == files.end() )
        return -ENOENT;

    if (!S_ISDIR(file[iter->second].mode))
        return -ENOTDIR;
//  TODO
//    if ( dir->ptr != NULL )
//        return -ENOTEMPTY;

    if ( strcmp(path, "/") == 0 )
        return -EBUSY;

    file [iter->second].inuse = 0;
    free(file [iter->second].data);
    file [iter->second].data = NULL;

    fs_stat.free_bytes = fs_stat.free_bytes + file [iter->second].size + sizeof ( file_pair );
    fs_stat.used_bytes = fs_stat.used_bytes - file [iter->second].size - sizeof ( file_pair );
    fs_stat.avail_no_of_files++;
    file [iter->second].size = 0;

    files.erase(iter);

    return 0;

}

int imfs_rename(const char *path, const char *newpath)
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
    if(S_ISDIR(file[iter->second].mode))
    {
        int len = strlen(path);
        for(files_iter iter = files.lower_bound(std::make_pair(path, "")); iter != files.end() && iter->first.first.substr(0, len) == path && (iter->first.first.length() == len || iter->first.first[len] == '/'); iter++)
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

static void imfs_destroy (void *tmp)
{
    for (int i = 0; i < fs_stat.max_no_of_files; i++ )
    {
        free(file [i].data);
    }

    free(file);

    files.clear();
}



int main(int argc, char *argv[])
{

    int size;
    int i = 2;

    if ( argc < 3 )
    {
        printf("%s <mountpoint> <size in (MB)>\n", argv[0]);
        exit(-1);
    }

    size = atoi(argv[2]);

    fs_stat.total_size = size * 1024 * 1024; /* In bytes */

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

    return fuse_main(argc, argv, &imfs_oper, NULL);
}
