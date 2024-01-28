# Makefile, Kevin Lundeen, Dominic Burgi, Seattle University, CPSC5300, Winter Quarter 2024
#
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib
SRC_DIR     = src
INC_DIR     = inc
BIN_DIR     = bin

# following is a list of all the compiled object files needed to build the sql5300 executable
OBJS       = $(BIN_DIR)/sql5300.o \
			 $(BIN_DIR)/sql_exec.o \
			 $(BIN_DIR)/heap_table.o \
			 $(BIN_DIR)/slotted_page.o \
			 $(BIN_DIR)/heap_file.o \
			 $(BIN_DIR)/heap_storage-test.o

# Rule for linking to create the executable
# Note that this is the default target since it is the first non-generic one in the Makefile: $ make
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

# General rule for compilation
$(BIN_DIR)/%.o: $(SRC_DIR)/%.cpp
	g++ -I$(INCLUDE_DIR) -I$(INC_DIR) $(CCFLAGS) -o "$@" "$<"

# Rule for removing all non-source files (so they can get rebuilt from scratch)
# Note that since it is not the first target, you have to invoke it explicitly: $ make clean
clean:
	rm -f sql5300 $(BIN_DIR)/*.o
