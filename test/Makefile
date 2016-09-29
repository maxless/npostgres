test: clean
	haxe -main Test -neko test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko test

stress: clean
	haxe -main StressTest -neko stress_test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko stress_test

batch: clean
	haxe -main BatchTest -neko batch_test.n -cp ../
	LD_PRELOAD=../npostgres/npostgres.ndll neko batch_test

test_cpp: clean
	haxe --remap neko:cpp -D HXCPP_M64 -D HXCPP_MULTI_THREADED -debug -main Test -cpp bin/ -cp ../
	LD_PRELOAD=../npostgres_cffi/npostgres.ndll bin/Test-debug

stress_cpp: clean
	haxe --remap neko:cpp -D HXCPP_M64 -D HXCPP_MULTI_THREADED -debug -main StressTest -cpp bin/ -cp ../
	LD_PRELOAD=../npostgres_cffi/npostgres.ndll bin/StressTest-debug

clean:
	rm -f *.n

all: test

