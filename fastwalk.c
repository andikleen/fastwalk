/* Print list of files for directory trees in dir order */
#define _GNU_SOURCE 1
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/fcntl.h>
#include <sys/state.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <unistd.h>

enum { 
	MAX_NAME = 256,
};

static int found_unknown;

struct entry { 
	ino_t ino;
	dev_t dev;
	char *name;
	unsigned long long disk;
	unsigned type;
};

static struct entry *entries;
static int maxentries, numentries;

static struct entry *getentry(void)
{
	if (numentries >= maxentries) {
		maxentries *= 2;
		entries = realloc(entries, maxentries * sizeof(struct entry));
	}
	return &entries[numentries++];
}

static void walk(char *dir)
{
	struct stat st, ste;
	int fd = open(dir, O_RDONLY|O_DIRECT);
	if (fd < 0 || fstat(fd, &st)) { 
		perror(name);
		goto out;
	}

	DIR *d = fdopendir(fd);
	
	int err;
	struct dirent *de, *dep;
	de = malloc(offsetof(struct dirent, d_name) + 
		    pathconf(dir, _PC_NAME_MAX) + 1); 
	while ((err = readdir_r(d, de, &dep)) == 0 && dep) { 
		char *name;

		asprintf(&name, "%s/%s", name, de->d_name);
		
		if (de->d_type == DT_DIR) { 
			walk(name);
			free(name);
		} else {
			struct entry *e = getentry(); 

			e->type = de->d_type;
			e->ino = st.st_ino;
			e->dev = st.st_dev;
			e->name = name;

			if (e->type == DT_UNKNOWN)
				found_unknown = 1;
		}
	} 
		
	free(de);
	closedir(d);
out:
	close(fd);
}

static int cmp_entry_ino(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
	if (a->dev != b->dev)
		return a->dev - b->dev;
	return a->ino - b->ino;
}


static int cmp_entry_disk(const void *av, const void *bv)
{
	const struct entry *a = av;
	const struct entry *b = bv;
	return a->disk - b->disk;
}

static void handle_unknown(void)
{
	int i;

	while (found_unknown) { 
		found_unknown = 0;
		for (i = 0; i < numentries; i++) {
			int preve = numentries;
			if (entries[i].type == DT_UNKNOWN) { 
				struct stat st;
				if (stat(entries[i].name, &st) < 0) {
					perror(entries[i].name); 
					continue;
				}
				if (IS_DIR(st.st_mode))
					walk(entries[i].name);
			}
			if (preve < numentries) 
				qsort(entries + preve, numentries - preve,
				      sizeof(struct entry), cmp_entry_ino);
		} 
	}

}

int main(int ac, char **av)
{
	int i;

	while (*++av) { 
		walk(*av); 
	}

	qsort(entries, numentries, sizeof(struct entry), cmp_entry_ino);
	
	handle_unknown();

	for (i = 0; i < numentries; i++) {
		int fd = open(entries[i].name, O_RDONLY);
		entries[i].disk = 0;
		if (ioctl(fd, FIBMAP, &entries[i].disk) < 0) 
			perror(entries[i].name); 
		close(fd);
	}
		
	qsort(entries, numentries, sizeof(struct entry), cmp_entry_disk);

	for (i = 0; i < numentries; i++)
		puts(entries[i].name);

	return 0;
}
