# Makefile, Duc Vo, Seattle University, CPSC5300, Winter Quarter 2024
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
CCFLAGS     = -std=c++11 -c -g
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

BIN_DIR     = ./bin
INC_DIR	 	= ./inc
SRC_DIR	 	= ./src

# Add suffixes to filenames to create the different lists of file types
FILES = slotted_page heap_file heap_table sql_exec schema_tables heap_storage storage_engine ParseTreeToString
HDRS 			= $(addsuffix .h, $(FILES))
OBJS 			= $(addsuffix .o, $(FILES)) sql5300.o
# Add paths to files to create the full paths`
HDRS_PATH  		= $(addprefix $(INC_DIR)/, $(HDRS))
OBJS_PATH  		= $(addprefix $(BIN_DIR)/, $(OBJS))


# Rule for linking to create the executable, note the full paths to the object files
sql5300: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS_PATH) -ldb_cxx -lsqlparser

# Rules for creating object files with headers
ParseTreeToString.o : $(HDRS_PATH)
sql_exec.o : $(HDRS_PATH)
slotted_page.o : $(HDRS_PATH)
heap_file.o : $(HDRS_PATH)
heap_table.o : $(HDRS_PATH)
schema_tables.o : $(HDRS_PATH)
sql5300.o : $(HDRS_PATH)
storage_engine.o : $(HDRS_PATH)

# General rule for compilation
%.o: $(SRC_DIR)/%.cpp
	g++ -I$(INCLUDE_DIR) -I$(INC_DIR) $(CCFLAGS) -o $(BIN_DIR)/$@ $<

# Rule for removing all non-source files (so they can get rebuilt from scratch)
clean:
	rm -f sql5300 $(BIN_DIR)/*.o
