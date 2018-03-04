# Hooray, a make file.

CC     = gcc
CFLAGS += -Wall -Wextra -std=gnu99 -pthread

SRCDIR       = src
LZ4_SRCS     = $(wildcard src/lz4/*.c)
ZLIB_SRCS    = $(wildcard src/zlib/*.c)
ZSTD_SRCS    = $(wildcard src/zstd/*.c)
OBJDIR       = obj
BINDIR       = bin

SOURCES  := $(wildcard $(SRCDIR)/*.c)
OBJECTS  := $(SOURCES:$(SRCDIR)/%.c=$(OBJDIR)/%.o)

# Targets
# The clock_gettime() on this gcc version requires -lrt.
build:
	$(CC) $(CFLAGS) -O3 -o $(BINDIR)/masters_project \
		$(LZ4_SRCS)                  \
		$(ZLIB_SRCS)                 \
		$(ZSTD_SRCS)                 \
		$(SRCDIR)/masters_project.c  \
		-lrt -lm

quick:
	$(CC) $(CFLAGS) -O0 -o $(BINDIR)/masters_project \
		$(LZ4_SRCS)                  \
		$(ZLIB_SRCS)                 \
		$(ZSTD_SRCS)                 \
		$(SRCDIR)/masters_project.c  \
		-lrt -lm

clean:
	rm -f $(BINDIR)/*
	rm -f $(OBJECTS)
	echo "Cleanup complete!"

