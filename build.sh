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
# setup a key for demo purposes
echo -n 'EKy(xjAnIfg6AT+OGd?nS1Mi5zZ&b*VXA@WxNLLE' > /tmp/key
echo "done compiling"
