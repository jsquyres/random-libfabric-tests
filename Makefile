CC = mpicc
CXX = mpiCC

LIBFABRIC_DIR = $(bogus)

CPPFLAGS = -I$(LIBFABRIC_DIR)/include -I.
CFLAGS = -g -O0 -Wall
CXXPPFLAGS = $(CPPFLAGS)
CXXFLAGS = $(CFLAGS) -std=gnu++11

LDFLAGS = -L$(LIBFABRIC_DIR)/lib
LIBS = -lfabric

PROGRAMS = \
        ofi-av-test \
        ofi-msg-fd-sockets-test \
        ofi-rdm-test \
        ofi-rdm-test-disconnect \
        ofi-test

all: $(PROGRAMS)

ofi-av-test: ofi-av-test.o
	$(CC) $(CFLAGS) $< $(LDFAGS) $(LIBS) -o $@
ofi-msg-fd-sockets-test: ofi-msg-fd-sockets-test.o
	$(CC) $(CFLAGS) $< $(LDFAGS) $(LIBS) -o $@
ofi-rdm-test: ofi-rdm-test.o
	$(CC) $(CFLAGS) $< $(LDFAGS) $(LIBS) -o $@
ofi-rdm-test-disconnect: ofi-rdm-test-disconnect.o
	$(CC) $(CFLAGS) $< $(LDFAGS) $(LIBS) -o $@
ofi-test: ofi-test.o
	$(CC) $(CFLAGS) $< $(LDFAGS) $(LIBS) -o $@

clean:
	rm -f *.o $(PROGRAMS) *~
