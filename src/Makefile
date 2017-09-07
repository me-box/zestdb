DESTDIR=/tmp
BIN=server.exe

.PHONY: build install uninstall clean

build:
		jbuilder build $(BIN)

clean:				
		rm -rf _build

install:	
		install ./_build/default/$(BIN) $(DESTDIR)

uninstall:
		rm -f $(DESTDIR)/$(BIN)

