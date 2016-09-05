test: clean
	haxe -main Test -neko test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko test

clean:
	rm -f test.n

all: test

