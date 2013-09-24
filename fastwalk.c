/* Copyright (c) 2010-2013 by Intel Corp.

   fastwalk is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public
   License as published by the Free Software Foundation; version
   2.

   fastwalk is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   General Public License for more details.

   You should find a copy of v2 of the GNU General Public License somewhere
   on your Linux system.

   Author:
   Andi Kleen */

/* Print list of files for directory trees in disk order of data on disk.
   Is careful to minimize seeks during operation, at the cost of some
   more CPU time. 

   The file list can be processed by a program that reads the file data
   with minimum seeks.

   Alternatively it can just start readaheads
  */
#define _GNU_SOURCE 1
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <linux/fiemap.h>
#include <unistd.h>
#include <errno.h>
#include <alloca.h>
#include <string.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/resource.h>
#include "list.h"

typedef unsigned long long u64;

struct fd;

struct entry { 
	ino_t ino;
	dev_t dev;
	unsigned type;
	char *name;
	union {
		struct fd *fd;
		u64 disk;
	};
	int numextents;
};

enum { 
	START_SIZE = (64 * 4096)/sizeof(struct entry),
	EXTENTS_START = 4096,
};

struct extent {
	u64 disk;
	u64 offset;
	u64 len;
	struct entry *entry;
};

struct fd { 
	struct entry *entry;
	struct list_head lru;
	int fd;
};

static struct entry *entries;
static int maxentries, numentries;

static struct extent *extents;
static int maxextents, numextents;

int error;

int debug;

int do_readahead;

#define Perror(x) (perror(x),error = 1)

static void oom(void)
{
	fprintf(stderr, "Out of memory\n");
	exit(ENOMEM);
}

static void *xrealloc(void *ptr, unsigned n)
{
	void *p = realloc(ptr, n);
	if (!p) oom();
	return p;
}

static void *xmalloc(unsigned n)
{
	void *p = malloc(n);
	if (!p) oom();
	return p;
}

static struct entry *getentry(void)
{
	struct entry *e;
	if (numentries >= maxentries) {
		if (maxentries == 0)
			maxentries = START_SIZE;
		else
			maxentries *= 2;
		entries = xrealloc(entries, maxentries * sizeof(struct entry));
	}
	e = &entries[numentries++];
	memset(e, 0, sizeof(struct entry));
	return e;
}

static int doskip(char *name, char **skip, int skipcnt)
{
	int k;

	for (k = 0; k < skipcnt; k++) {
		if (!strcmp(name, skip[k]))
			return 1;
	}
	return 0;
}

static int walk(char *dir, char **skip, int skipcnt)
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
		close(fd);
		return found_unknown;
	}

	DIR *d = fdopendir(fd);
	
	struct dirent *de;
	de = alloca(offsetof(struct dirent, d_name) + 
		    pathconf(dir, _PC_NAME_MAX) + 1); 
	while (readdir_r(d, de, &de) == 0 && de) { 
		char *name;

		if (asprintf(&name, "%s/%s", dir, de->d_name) < 0) 
			oom();

		if (doskip(de->d_name, skip, skipcnt))
			continue;

		if (de->d_type == DT_DIR) { 
			if (walk(name, skip, skipcnt))
				found_unknown = 1;
			free(name);
		} else {
			struct entry *e = getentry(); 

			e->type = de->d_type;
			e->ino = de->d_ino;
			e->dev = st.st_dev;
			e->name = name;

			if (e->type == DT_UNKNOWN) {
				found_unknown = 1;
				if (debug)
					fprintf(stderr, "%s: DT_UNKNOWN\n", name);
			}
		}
	} 
		
	closedir(d);
	return found_unknown;
}

static int cmp_entry_ino(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
	return a->ino - b->ino;
}

static int cmp_entry_disk(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
	return a->disk - b->disk;
}

static int cmp_extent(const void *av, const void *bv)
{
	const struct extent *a = av;
	const struct extent *b = bv;
	return a->disk - b->disk;
}

static void sort_inodes(void)
{
	qsort(entries, numentries, sizeof(struct entry), cmp_entry_ino);
}

/* Sort entry by disk order. Only for the first extent */
static void sort_entries_disk(void)
{
	int i;
	for (i = 0; i < numextents; i++)
		extents[i].entry->disk = extents[i].disk;
	qsort(entries, numentries, sizeof(struct entry), cmp_entry_disk);
}

/* Sort extents by disk order */
static void sort_extents(void)
{
	qsort(extents, numextents, sizeof(struct extent), cmp_extent);
}

static void handle_unknown(char **skip, int skipcnt)
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
			if (S_ISDIR(st.st_mode) && walk(entries[i].name, 
							skip, skipcnt))
				found_unknown = 1;
		}

		if (!found_unknown) 
			break;
		
		sort_inodes();
		start = max;
	} 
}

static struct extent *get_extents(int num)
{
	struct extent *e;
	
	while (numextents + num > maxextents) { 
		if (maxextents == 0)
			maxextents = EXTENTS_START;
		maxextents *= 2;
		extents = xrealloc(extents, maxextents * sizeof(struct extent));
	}
	e = extents + numextents;
	numextents += num;
	return e;
}

static void save_extents(struct fiemap *fie, struct entry *entry)
{
	struct extent *e;
	int i;
	int num = do_readahead ? fie->fm_mapped_extents : 1; 

	e = get_extents(num);
	for (i = 0; i < num; i++, e++) { 
		if (fie->fm_extents[i].fe_flags & FIEMAP_EXTENT_UNKNOWN) {
			memset(e, 0, sizeof(struct extent));
			e->entry = entry;
			continue;
		}
		e->disk = fie->fm_extents[i].fe_physical;
		e->offset = fie->fm_extents[i].fe_logical;
		e->len = fie->fm_extents[i].fe_length;
		e->entry = entry;
	}
	entry->numextents = num;
}

static void get_disk(char *name, int fd, struct entry *entry)
{
	u64 disk;
	static int once;
	const int N = 100;
	struct fiemap *fie = alloca(sizeof(struct fiemap) + 
				    sizeof(struct fiemap_extent)*N);
	struct stat st;

	if (stat(name, &st) < 0) { 
		Perror(name);
		return;
	}

	memset(fie, 0, sizeof(struct fiemap));
	fie->fm_extent_count = N;
	fie->fm_start = 0;
	fie->fm_length = st.st_size;
	
	/* If the extents have out of inode contents we will seek here.
	   No way to avoid that currently */

	if (ioctl(fd, FS_IOC_FIEMAP, fie) >= 0) {
		if (fie->fm_flags & FIEMAP_EXTENT_UNKNOWN) { 
			if (!once)
				fprintf(stderr, "%s: Disk location unknown\n", name); 
			once = 1;
		}
		save_extents(fie, entry);
		return;
	}
	
	if (ioctl(fd, FIBMAP, &disk) < 0) {
		if (errno == EPERM) {
			if (!once) 
				fprintf(stderr, 
					"%s: No FIEMAP and no root: no disk data sorting\n", name);
			once = 1;
		}
		memset(fie, 0, 
		       sizeof(struct fiemap) + sizeof(struct fiemap_extent));
		fie->fm_mapped_extents = 1;
		fie->fm_extents[0].fe_physical = st.st_size;
		save_extents(fie, entry);
	}
}

/* LRU for file descriptors */

static LIST_HEAD(lru);
static struct fd *fds;
static int free_fd, max_fd;

static int list_len(struct list_head *h, int *freep)
{
	struct list_head *l;
	int i = 0;
	*freep = 0;
	list_for_each (l, h) {
		struct fd *fd = list_entry(l, struct fd, lru);
		if (!fd->entry) (*freep)++;
		i++;
	}
	return i;
}

static void log_lru(void)
{
	static FILE *f; 
	int fl = 0;
	if (!f) f = fopen("/tmp/lru", "w");
	int len = list_len(&lru, &fl);
	fprintf(f, "%d %d\n", len, fl);
}

static void init_fd(void)
{
	struct rlimit rlim;
	if (getrlimit(RLIMIT_NOFILE, &rlim) < 0)
		rlim.rlim_cur = 100;
	max_fd = rlim.rlim_cur;
	max_fd -= max_fd / 10; /* save 10% for safety */
	fds = xmalloc(sizeof(struct fd) * max_fd);
}

static void do_close_fd(struct fd *fd)
{
	close(fd->fd);
	fd->entry->fd = NULL;
	fd->entry = NULL;
}

static struct fd *get_unused_fd(void)
{
	struct fd *fd;
	if (free_fd < max_fd)
		return &fds[free_fd++];
	assert(!list_empty(&lru));
	fd = list_entry(lru.prev, struct fd, lru);
	list_del(&fd->lru);
	if (fd->entry)
		do_close_fd(fd);
	return fd;
}

static struct fd *get_fd(struct entry *e)
{
	struct fd *fd = e->fd;
	if (fd) {
		list_del(&fd->lru);
	} else {
		fd = get_unused_fd();
		fd->fd = open(e->name, O_RDONLY);
		if (fd->fd < 0) { 
			list_add_tail(&fd->lru, &lru);
			return NULL;
		} else { 
			e->fd = fd;
		}
		fd->entry = e;
	}
	list_add(&fd->lru, &lru);
	return fd;
}

static void close_fd(struct fd *fd)
{
	do_close_fd(fd);
	list_del(&fd->lru);
	list_add_tail(&fd->lru, &lru);
}

static void usage(void)
{
	fprintf(stderr, "Usage: fastwalk [-pSKIP] [-r]\n"
			"Generate list of files in (approx) logical disk order to minimize seeks.\n"
			"By default a list of names is generated, that can be\n"
		       	"read by another program\n"
			"\n"
			"-pSKIP skip files/directories named SKIP\n"
			"-r     read ahead files instead of outputting name\n");
	exit(1);
}

int main(int ac, char **av)
{
	int i;
	int found_unknown = 0;
	int opt;
	char *skip[ac + 2];
	int skipcnt = 0;

	skip[skipcnt++] = ".";
	skip[skipcnt++] = "..";
	while ((opt = getopt(ac, av, "p:r")) != -1) {
		switch (opt) { 
		case 'p':
			skip[skipcnt++] = optarg;
			break;
		case 'r':
			do_readahead = 1;
			break;
		case 'd':
			debug++;
			break;
		default:
			usage();
		}
	}

	/* First pass: read directories */
	if (optind == ac) {
		walk(".", skip, skipcnt);
	} else { 
		for (i = optind; i < ac; i++) { 
			if (walk(av[i], skip, skipcnt))
				found_unknown = 1;
		}
	}

	/* Inode sort for fast stat */
	sort_inodes();
	
	/* For DT_UNKNOWN file systems complete the tree */
	if (found_unknown)
		handle_unknown(skip, skipcnt);

	/* Second pass: Get disk addresses: reads inodes and extents.
	   The extent reading is not necessarily in disk order
	   because the kernel doesn't give us this currently. 
	   But it should work for the common case of the extents
	   (or indirect blocks) being inlined into the inode. */
	for (i = 0; i < numentries; i++) {
		int fd;
		if (entries[i].type != DT_REG)
			continue;
		fd = open(entries[i].name, O_RDONLY);
		if (fd >= 0) {
			get_disk(entries[i].name, fd, &entries[i]);
			close(fd);
		} else {
			Perror(entries[i].name);
		}
	}

	if (do_readahead) {
		init_fd();
		
		sort_extents();

		/* Third pass: read the data in disk order */
		for (i = 0; i < numextents; i++) {
			struct extent *ex = &extents[i];
			struct entry *e = ex->entry;
			struct fd *fd = get_fd(e);

			if (debug > 0)	
				log_lru();
			if (!fd) { 
				Perror(e->name);
				continue;
			}
			readahead(fd->fd, ex->offset, ex->len);
			if (--e->numextents == 0)
				close_fd(fd);
		}
	} else {
		sort_entries_disk();

		for (i = 0; i < numentries; i++)
			puts(entries[i].name);
	}

	return error;
}
