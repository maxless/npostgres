test: clean
	haxe -main Test -neko test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko test

stress: clean
	haxe -main StressTest -neko stress_test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko stress_test

clean:
	rm -f *.n

all: test

