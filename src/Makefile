DESTDIR=/tmp
BIN=server.exe

.PHONY: build install uninstall clean

build:
		jbuilder build $(BIN)

clean:				
		rm -rf _build *_store

install:	
		install ./_build/default/$(BIN) $(DESTDIR)

uninstall:
		rm -f $(DESTDIR)/$(BIN)

