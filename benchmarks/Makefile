ghc := ghc
ghc-flags :=
programs := builder bench

.PHONY: all
all: $(programs)

builder: Builder.hs
	# $(ghc) $(ghc-flags) --make -O2 Builder.hs -o $@ -fforce-recomp -i../src
	$(ghc) $(ghc-flags) --make -O2 Builder.hs -o $@ -fforce-recomp

bench: Benchmark.hs MemBench.hs CBenchmark.o
	# $(ghc) $(ghc-flags) --make -O2 -fliberate-case-threshold=1000 -fasm Benchmark.hs CBenchmark.o -o $@ -fforce-recomp -i../src
	$(ghc) $(ghc-flags) --make -O2 -fliberate-case-threshold=1000 -fasm Benchmark.hs CBenchmark.o -o $@ -fforce-recomp

.PHONY: run-bench
run-bench: bench
	./bench 100

CBenchmark.o: CBenchmark.c
	$(ghc) -c -optc -O3 $< -o $@

.PHONY: clean
clean:
	rm -f *.o *.hi $(programs)


# --- old builder
# 
# benchmarking [Word8]
# mean: 49.26234 us, lb 49.14113 us, ub 49.55259 us, ci 0.950
# std dev: 904.8610 ns, lb 358.2825 ns, ub 1.666588 us, ci 0.950
# 
# benchmarking bounds/[Word8]
# mean: 94.91118 us, lb 94.59910 us, ub 95.85672 us, ci 0.950
# std dev: 2.528794 us, lb 897.7127 ns, ub 5.486742 us, ci 0.950
# 
# benchmarking small ByteString
# mean: 285.1628 ns, lb 283.4856 ns, ub 289.4121 ns, ci 0.950
# std dev: 12.67753 ns, lb 5.224720 ns, ub 24.60277 ns, ci 0.950
# 
# benchmarking large ByteString
# mean: 288.2415 ns, lb 287.2276 ns, ub 290.6641 ns, ci 0.950
# std dev: 7.545355 ns, lb 3.697763 ns, ub 14.69685 ns, ci 0.950
# 
# benchmarking length-prefixed ByteString
# mean: 7.947424 us, lb 7.921150 us, ub 8.016153 us, ci 0.950
# std dev: 201.0435 ns, lb 93.08679 ns, ub 404.6773 ns, ci 0.950
# 
# benchmarking Host endian/1MB of Word8 in chunks of 16
# mean: 3.355074 ms, lb 3.345878 ms, ub 3.379972 ms, ci 0.950
# std dev: 71.98007 us, lb 30.44320 us, ub 151.2873 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word16 in chunks of 16
# mean: 1.648855 ms, lb 1.643633 ms, ub 1.661289 ms, ci 0.950
# std dev: 38.70837 us, lb 17.11387 us, ub 71.75939 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word32 in chunks of 16
# mean: 863.3337 us, lb 861.6389 us, ub 867.1304 us, ci 0.950
# std dev: 12.39763 us, lb 6.562405 us, ub 23.33076 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word64 in chunks of 16
# mean: 449.6739 us, lb 448.6307 us, ub 452.3396 us, ci 0.950
# std dev: 7.957488 us, lb 2.726459 us, ub 15.67763 us, ci 0.950
# 
# 
# --- new builder
# 
# benchmarking [Word8]
# mean: 183.8225 us, lb 182.7893 us, ub 185.3910 us, ci 0.950
# std dev: 6.414245 us, lb 4.518765 us, ub 10.47748 us, ci 0.950
# 
# benchmarking bounds/[Word8]
# mean: 416.9508 us, lb 415.1468 us, ub 419.1800 us, ci 0.950
# std dev: 10.20582 us, lb 7.781716 us, ub 15.04081 us, ci 0.950
# 
# benchmarking small ByteString
# mean: 421.2735 ns, lb 419.8489 ns, ub 425.0524 ns, ci 0.950
# std dev: 10.99856 ns, lb 4.412237 ns, ub 22.64138 ns, ci 0.950
# 
# benchmarking large ByteString
# mean: 429.2569 ns, lb 428.2206 ns, ub 431.1254 ns, ci 0.950
# std dev: 6.910428 ns, lb 4.499563 ns, ub 10.66732 ns, ci 0.950
# 
# benchmarking length-prefixed ByteString
# mean: 5.270638 us, lb 5.256502 us, ub 5.299993 us, ci 0.950
# std dev: 100.1264 ns, lb 47.20407 ns, ub 170.4914 ns, ci 0.950
# 
# benchmarking Host endian/1MB of Word8 in chunks of 16
# mean: 11.49178 ms, lb 11.46305 ms, ub 11.53991 ms, ci 0.950
# std dev: 186.3255 us, lb 127.5454 us, ub 272.8612 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word16 in chunks of 16
# mean: 5.738518 ms, lb 5.722276 ms, ub 5.763683 ms, ci 0.950
# std dev: 102.3286 us, lb 72.04613 us, ub 144.3739 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word32 in chunks of 16
# mean: 2.980609 ms, lb 2.970755 ms, ub 3.004359 ms, ci 0.950
# std dev: 73.64776 us, lb 34.03438 us, ub 146.0554 us, ci 0.950
# 
# benchmarking Host endian/1MB of Word64 in chunks of 16
# mean: 1.447254 ms, lb 1.442844 ms, ub 1.458563 ms, ci 0.950
# std dev: 33.00866 us, lb 8.002687 us, ub 62.59927 us, ci 0.950
# 
