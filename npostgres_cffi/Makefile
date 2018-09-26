# change these to where your libpq and neko include dirs are
NEKODIR = /usr/lib/neko/include
PGSQLDIR = /usr/include/postgresql
HXCPPDIR = $(shell haxelib path hxcpp | head -1)/include

#ARCH=-m32
ARCH=-m64

CC = g++

#DEBUG = -g
DEBUG =
CFLAGS = --std=c++0x -w -fPIC -I $(PGSQLDIR) -I $(NEKODIR) -I $(HXCPPDIR) -I . $(DEBUG)
#LDFLAGS = -shared -Wl,-soname,npostgres.ndll -lc -lpq $(DEBUG)
LDFLAGS = -shared -Wl,-soname,npostgres.ndll,--no-as-needed -lc -lpq $(DEBUG)

npostgres.ndll: postgres.o
	$(CC) $(LDFLAGS) $(ARCH) -o npostgres.ndll postgres.o

postgres.o: postgres.cpp
	$(CC) $(CFLAGS) $(ARCH) -c postgres.cpp -o postgres.o 

all: npostgres.ndll

clean:
	rm postgres.o npostgres.ndll
