# change these to where your libpq and neko include dirs are
NEKODIR = /usr/lib/neko/include
PGSQLDIR = /usr/include/postgresql

#ARCH=-m32
ARCH=-m64

CC = g++

#DEBUG = -g
DEBUG =
CFLAGS = -w -fPIC -I $(PGSQLDIR) -I $(NEKODIR) -I . $(DEBUG)
LDFLAGS = -shared -Wl,-soname,npostgres.ndll -lc -lpq $(DEBUG)

npostgres.ndll: postgres.o
	$(CC) $(LDFLAGS) $(ARCH) -o npostgres.ndll postgres.o

postgres.o: postgres.cpp
	$(CC) $(CFLAGS) $(ARCH) -c postgres.cpp -o postgres.o 

all: npostgres.ndll

clean:
	rm postgres.o npostgres.ndll
