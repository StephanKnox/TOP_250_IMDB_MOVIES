#!/bin/bash
# $1 - filename to split
# $2 - file prefix for split files

# split input file into new file each 50 lines + header from the original file
tail -n +3 $1 | split -l 50 - $2
for file in $2*
do
    head -n 1 $1 > tmp_file
    cat "$file" >> tmp_file
    mv -f tmp_file "$file"
done
