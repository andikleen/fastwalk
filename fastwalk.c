/* Print list of files for directory trees in disk order of data on disk.
   Is careful to minimize seeks during operation, at the cost of some
   more CPU time. 

   The file list can be processed by a program that reads the file data
   with minimum seeks

  */
#define _GNU_SOURCE 1
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <unistd.h>
#include <errno.h>
#include <alloca.h>
#include <string.h>

struct entry { 
	ino_t ino;
	dev_t dev;
	unsigned type;
	char *name;
	unsigned long long disk;
};

enum { 
	START_SIZE = (64 * 4096)/sizeof(struct entry),
};

static struct entry *entries;
static int maxentries, numentries;
int error;

#define Perror(x) perror(x),error++

static void oom(void)
{
	fprintf(stderr, "Out of memory\n");
	exit(ENOMEM);
}

static struct entry *getentry(void)
{
	if (numentries >= maxentries) {
		if (maxentries == 0)
			maxentries = START_SIZE;
		else
			maxentries *= 2;
		entries = realloc(entries, maxentries * sizeof(struct entry));
		if (!entries)
			oom();
	}
	return &entries[numentries++];
}

static int walk(char *dir)
{
	int found_unknown = 0;
	struct stat st;
	int fd = open(dir, O_RDONLY|O_DIRECTORY);
	if (fd < 0) { 
		Perror(dir);
		return 0;
	}
	if (fstat(fd, &st) < 0) { 
		Perror(dir);
		goto out;
	}

	DIR *d = fdopendir(fd);
	
	int err;
	struct dirent *de;
	de = alloca(offsetof(struct dirent, d_name) + 
		    pathconf(dir, _PC_NAME_MAX) + 1); 
	while ((err = readdir_r(d, de, &de)) == 0 && de) { 
		char *name;

		if (asprintf(&name, "%s/%s", dir, de->d_name) < 0) 
			oom();
		
		if (de->d_name[0] == '.' &&
		    (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")))
			continue;

		if (de->d_type == DT_DIR) { 
			if (walk(name))
				found_unknown = 1;
			free(name);
		} else {
			struct entry *e = getentry(); 

			e->type = de->d_type;
			e->ino = de->d_ino;
			e->dev = st.st_dev;
			e->name = name;
			e->disk = 0;

			if (e->type == DT_UNKNOWN) {
				found_unknown = 1;
				fprintf(stderr, "%s: DT_UNKNOWN\n", name);
			}
		}
	} 
		
	closedir(d);
out:
	close(fd);
	return found_unknown;
}

static int cmp_entry_ino(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
#if 0
	if (a->dev != b->dev)
		return a->dev - b->dev;
#endif
	return a->ino - b->ino;
}

static int cmp_entry_disk(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
#if 0
	if (a->dev != b->dev)
		return a->dev - b->dev;
#endif
	return a->disk - b->disk;
}

static void sort_inodes(void)
{
	qsort(entries, numentries, sizeof(struct entry), cmp_entry_ino);
}

static void handle_unknown(void)
{
	int i, start, max, found_unknown;

	fprintf(stderr, "Warning: file system does not support dt_type\n");
 
	found_unknown = 0;
	start = 0;
	for (;;) { 
		max = numentries;
		for (i = start; i < max; i++) {
			struct stat st;

			if (entries[i].type != DT_UNKNOWN)
				continue;
			if (stat(entries[i].name, &st) < 0) {
				Perror(entries[i].name); 
				continue;
			}
			if (S_ISDIR(st.st_mode) && walk(entries[i].name))
				found_unknown = 1;
		}

		if (!found_unknown) 
			break;
		
		sort_inodes();
		start = max;
	} 
}

static void get_disk(char *name, int fd, unsigned long long *disk)
{
	static int once;
	const int N = 1;
	struct fiemap *fie = alloca(sizeof(struct fiemap) + 
				    sizeof(struct fiemap_extent)*N);

	memset(fie, 0, sizeof(struct fiemap));
	fie->fm_extent_count = N;
	fie->fm_length = 4096; /* Assume the file is continuous */
	if (ioctl(fd, FS_IOC_FIEMAP, fie) >= 0) {
		if (fie->fm_flags & FIEMAP_EXTENT_UNKNOWN) { 
			if (!once)
				fprintf(stderr, "%s: Disk location unknown\n", name); 
			once = 1;
		}
		if (fie->fm_mapped_extents > 0)
			*disk = fie->fm_extents[0].fe_physical;
		return;
	}
	
	if (ioctl(fd, FIBMAP, disk) < 0) {
		if (errno == EPERM) {
			if (!once) 
				fprintf(stderr, 
					"%s: No FIEMAP and no root: no disk data sorting\n", name);
			once = 1;
		}
	}
}

int main(int ac, char **av)
{
	int i;
	int found_unknown = 0;

	while (*++av) { 
		if (walk(*av))
			found_unknown = 1;
	}

	/* First pass: inode sort for fast stat */
	sort_inodes();
	
	/* For DT_UNKNOWN file systems complete the tree */
	if (found_unknown)
		handle_unknown();

	/* Get disk addresses */
	for (i = 0; i < numentries; i++) {
		int fd;
		if (entries[i].type != DT_REG)
			continue;
		fd = open(entries[i].name, O_RDONLY);
		if (fd >= 0) {
			get_disk(entries[i].name, fd, &entries[i].disk);
			close(fd);
		} else
			Perror(entries[i].name);
	}
		
	/* Sort by disk order */
	qsort(entries, numentries, sizeof(struct entry), cmp_entry_disk);

	for (i = 0; i < numentries; i++)
		puts(entries[i].name);

	return error;
}
