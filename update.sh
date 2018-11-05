rm -rf temp
mkdir temp
cd temp
wget -q -O sqlite.tar.gz https://www.sqlite.org/src/tarball/sqlite.tar.gz?r=release
tar -zxvf sqlite.tar.gz
mkdir build
cd build
../sqlite/configure
make sqlite3.c
cd ../../
cp temp/build/sqlite3.h ./sqlite3.h
cp temp/build/sqlite3.c ./sqlite3.c