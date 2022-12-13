#!/bin/bash

./gensort -a -b0 320000 slave1_input1
./gensort -a -b320000 320000 slave1_input2
./gensort -a -b640000 320000 slave1_input3
./gensort -a -b960000 320000 slave1_input4
./gensort -a -b1280000 320000 slave1_input5
./gensort -a -b1600000 320000 slave2_input1
./gensort -a -b1920000 320000 slave2_input2
./gensort -a -b2240000 320000 slave2_input3
./gensort -a -b2560000 320000 slave2_input4
./gensort -a -b2880000 320000 slave2_input5
./gensort -a -b3200000 320000 slave3_input1
./gensort -a -b3520000 320000 slave3_input2
./gensort -a -b3840000 320000 slave3_input3
./gensort -a -b4160000 320000 slave3_input4
./gensort -a -b4480000 320000 slave3_input5
./gensort -a -b4800000 320000 slave4_input1
./gensort -a -b5120000 320000 slave4_input2
./gensort -a -b5440000 320000 slave4_input3
./gensort -a -b5760000 320000 slave4_input4
./gensort -a -b6080000 320000 slave4_input5
./gensort -a -b6400000 320000 slave5_input1
./gensort -a -b6720000 320000 slave5_input2
./gensort -a -b7040000 320000 slave5_input3
./gensort -a -b7360000 320000 slave5_input4
./gensort -a -b7680000 320000 slave5_input5
./gensort -a -b8000000 320000 slave6_input1
./gensort -a -b8320000 320000 slave6_input2
./gensort -a -b8640000 320000 slave6_input3
./gensort -a -b8960000 320000 slave6_input4
./gensort -a -b9280000 320000 slave6_input5
./gensort -a -b9600000 320000 slave7_input1
./gensort -a -b9920000 320000 slave7_input2
./gensort -a -b10240000 320000 slave7_input3
./gensort -a -b10560000 320000 slave7_input4
./gensort -a -b10880000 320000 slave7_input5
./gensort -a -b11200000 320000 slave8_input1
./gensort -a -b11520000 320000 slave8_input2
./gensort -a -b11840000 320000 slave8_input3
./gensort -a -b12160000 320000 slave8_input4
./gensort -a -b12480000 320000 slave8_input5
./gensort -a -b12800000 320000 slave9_input1
./gensort -a -b13120000 320000 slave9_input2
./gensort -a -b13440000 320000 slave9_input3
./gensort -a -b13760000 320000 slave9_input4
./gensort -a -b14080000 320000 slave9_input5

scp slave1_* vm03:/home/blue/input/
scp slave2_* vm04:/home/blue/input/
scp slave3_* vm05:/home/blue/input/
scp slave4_* vm06:/home/blue/input/
scp slave5_* vm07:/home/blue/input/
scp slave6_* vm08:/home/blue/input/
scp slave7_* vm09:/home/blue/input/
scp slave8_* vm10:/home/blue/input/
scp slave9_* vm11:/home/blue/input/
