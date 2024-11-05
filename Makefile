# Compiler
CC = gcc

# Compiler Flags
CFLAGS = -Wall -Wextra -pthread -g

# Executable name
EXEC = wordcount

# Source files
SRCS = distwc.c mapreduce.c threadpool.c

# Object files
OBJS = $(SRCS:.c=.o)

# Directory containing test input files
INPUT_DIR = sample_inputs

# Default target
all: $(EXEC)

# Target to create the executable
$(EXEC): $(OBJS)
	$(CC) $(CFLAGS) -o $(EXEC) $(OBJS)

# Compile each source file into an object file
%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

# Run target to execute wordcount on all files in sample_inputs
run: $(EXEC)
	@echo "Running $(EXEC) on all files in $(INPUT_DIR):"
	@./$(EXEC) $(INPUT_DIR)/*

# Clean target to remove object files and the executable
clean:
	rm -f $(OBJS) $(EXEC)