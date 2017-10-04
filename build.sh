#! /bin/sh

# compile code
echo "compiling..."
eval `opam config env`
cd src && jbuilder build server.exe
cp base-cat.json ../
cp ./_build/default/server.exe ../
cd ../test && jbuilder build client.exe
cp ./_build/default/client.exe ../
cd ../utils && jbuilder build mint.exe
cp ./_build/default/mint.exe ../
echo "done compiling"