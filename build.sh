#! /bin/sh

# compile code
echo "compiling..."
eval `opam config env`
cd src && jbuilder build server.exe
cp base-cat.json ../
cp ./_build/default/server.exe ../
cd ../test && jbuilder build client.exe
cp ./_build/default/client.exe ../
echo "done compiling"