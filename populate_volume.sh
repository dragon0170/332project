#!/bin/sh

mkdir input_dir1
./gensort -a -b0 320000 input1
./gensort -a -b320000 320000 input2
mv input1 input_dir1
mv input2 input_dir1
mkdir input_dir2
#./gensort -a -b640000 320000 input3
#./gensort -a -b960000 320000 input4
mv input3 input_dir2
mv input4 input_dir2
mkdir output_slave_1
chmod 777 output_slave_1

mkdir input_dir3
./gensort -a -b640000 320000 input1
./gensort -a -b960000 320000 input2
mv input1 input_dir3
mv input2 input_dir3
mkdir input_dir4
#./gensort -a -b600 100 input3
#./gensort -a -b700 100 input4
mv input3 input_dir4
mv input4 input_dir4
mkdir output_slave_2
chmod 777 output_slave_2

mkdir input_dir5
./gensort -a -b1280000 320000 input1
./gensort -a -b1600000 320000 input2
mv input1 input_dir5
mv input2 input_dir5
mkdir input_dir6
#./gensort -a -b1000 100 input3
#./gensort -a -b1100 100 input4
mv input3 input_dir6
mv input4 input_dir6
mkdir output_slave_3
chmod 777 output_slave_3
