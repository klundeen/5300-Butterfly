CXX = g++
CXXFLAGS = -I/usr/local/db6/include -I$(INC_DIR) -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -std=c++11
LDFLAGS = -L/usr/local/db6/lib
LD_LIBRARY_PATH = -L/usr/local/db6/lib
LIBS = -ldb_cxx -lsqlparser
TARGET = sql_db
SRC_DIR = src
OBJ_DIR = bin
INC_DIR = inc

SRCS = $(wildcard $(SRC_DIR)/*cpp)
INCS = $(wildcard $(INC_DIR)/*.h)
OBJS = $(patsubst $(SRC_DIR)/%.cpp,$(OBJ_DIR)/%.o,$(SRCS))

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(LIBS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cpp $(INCS)
	$(CXX) $(CXXFLAGS) -c -o $@ $<

clean : 
	rm -f $(OBJS) $(TARGET)
