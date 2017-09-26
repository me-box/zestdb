#! /bin/sh

# compile code
echo "compiling..."
eval `opam config env`
cd src && jbuilder build server.exe
echo "done compiling"
# setup runtime env
cp base-cat.json ../
ln ./_build/default/server.exe ../server.exe
