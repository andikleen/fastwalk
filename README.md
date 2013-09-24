# fastwalk

## Introduction

fastwalk is simple Linux utility to walk directory trees in logical
disk order. Best on spinning disks.A simple usage would be to speed up
builds (and other operations on whole directory trees) on spinning
disks by reading all the files into the file cache first, before
starting the build. The readahead can happen parallel to the main
operations.  It is far more efficient to do this readahead in disk
order to minimize seeks, and fast walk generates the right file order
for this.

By default it primes the inode and directory caches of the kernel,
and then outputs the list of file names in disk order. Alternatively
(with the -r option) it can also start readahead for the file 
contents

## Usage

To build

	make
	cp fastwalk $prefix/bin
	cp fastwalk.1 $prefix/man/man1

For example speeding up large builds (assuming you have enough memory):

	cd my-big-project
	fastwalk -r . &
	make ...

fastwalk will start the readahead in the background to load all files.
This works best if you work with a separate object directory and do
not include the object files in the fast path walk.

I also use it to speedup large greps or indexing operations on 
source trees (it makes GNU grep mostly competive with git grep
in performance on spinning disks)
	
All options

	fastwalk [-r] [-p skipdir]  dir ...

	-p skipdir adds directory names to skip.
	-r start readahead of the file contents

## Caveats

It works best on file systems with a classical BSD style layout, like
ext*. XFS has some limitations (lack of DT_* types), also does btrfs.

Andi Kleen
