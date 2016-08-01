CC = mpicc
CXX = mpiCC

LIBFABRIC_DIR = $(bogus)

CPPFLAGS = -I$(LIBFABRIC_DIR)/include -I.
CFLAGS = -g -O0 -Wall
CXXPPFLAGS = $(CPPFLAGS)
CXXFLAGS = $(CFLAGS) -std=gnu++11

LDFLAGS = -L$(LIBFABRIC_DIR)/lib
LIBS = -lfabric

PROGRAMS = abuse

all: $(PROGRAMS)

main.cc: abuse.h
client.cc: abuse.h
server.cc: abuse.h

abuse: main.o client.o server.o
	$(CXX) $(CXXFLAGS) $^ $(LDFAGS) $(LIBS) -o $@

clean:
	rm -f *.o $(PROGRAMS) *~
