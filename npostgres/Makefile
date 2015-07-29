# change these to where your libpq and neko include dirs are
NEKODIR = /usr/lib/neko/include
PGSQLDIR = /usr/include/postgresql

CC = gcc

CFLAGS = -w -fPIC -I $(PGSQLDIR) -I $(NEKODIR)
#LDFLAGS = -shared -Wl,-soname,npostgres.ndll -lc -lpq
LDFLAGS = -shared -Wl,-soname,npostgres.ndll,--no-as-needed -lc -lpq

npostgres.ndll: postgres.o
	$(CC) $(LDFLAGS) -o npostgres.ndll postgres.o

postgres.o: postgres.c
	$(CC) $(CFLAGS) -c postgres.c -o postgres.o 

all: npostgres.ndll

clean:
	rm postgres.o npostgres.ndll
