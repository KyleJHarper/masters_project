/*
 *  This is the main program file for a Master's Project to acquire metrics on compression characteristics of various pieces of
 *  data.  For simplicity we will likely use a single code file, and only use headers/prototypes for 3rd party components
 *  (i.e.: you'll find main() at the bottom).
 *
 *  !!! BIG IMPORTANT NOTES !!!
 *  This is a proof of concept; we eschew certain paradigms for simplicity.
 *  !!! PORTABILITY !!!
 *  This is tested on LINUX using GCC.  If it fails on <some_other_platform>, sorry.
 *  !!! SECURITY !!!
 *  This is a proof of concept, and therefore doesn't enforce any security.  Specifically, we will printf user input
 *  which is a great way to allow vulnerabilities (injections, NOP-sleds, etc).
 */

// Includes
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include "lz4/lz4.h"
#include "zlib/zlib.h"
#include "zstd/zstd.h"
#include <time.h>
#include <libgen.h>

// Defines & Globals
#define E_GENERIC            1
#define E_IO                 2
#define MAX_FILES           32
#define MAX_BUFFERS     500000   // This is 500,000 * 2048 bytes == 1GB maximum supported file.
#define MILLION        1000000L
#define BILLION     1000000000L
#define BLOCK_COUNT          5
#define ZSTD_LEVEL           3   // ZSTD Default
#define ZLIB_LEVEL           6   // Gzip Default
const int block_sizes[BLOCK_COUNT] = {2048, 4096, 8192, 16384, 32768};

// Structs
typedef struct src_file src_file;
struct src_file {
  char     *filespec;  // The path to the file, fully qualified.
  void     *data;      // The data.
  uint64_t size;   // The length of the data.
};
typedef struct result result;
struct result {
  src_file *src;  // Just link to the original since it should live the whole program life.
  int      block_size;
  int      blocks;
  uint64_t memcpy_time;
  uint64_t lz4_comp_size;
  uint64_t lz4_comp_time;
  uint64_t lz4_decomp_time;
  uint64_t zlib_comp_size;
  uint64_t zlib_comp_time;
  uint64_t zlib_decomp_time;
  uint64_t zstd_comp_size;
  uint64_t zstd_comp_time;
  uint64_t zstd_decomp_time;
};
typedef struct buffer buffer;
struct buffer {
  void     *raw;
  void     *compressed;
  void     *decompressed;
  uint64_t raw_size;
  int64_t  comp_size;
};






/*
 *  Simple Error & Quit Function
 */
void fatal(int exit_code, char *format, ...) {
  va_list list;
  fprintf(stderr, "ERROR: ");
  va_start(list, format);
  vfprintf(stderr, format, list);
  va_end(list);
  fprintf(stderr, "\nABORTING\n");
  if (exit_code != 0)
    exit(exit_code);
  printf("The show_error function was given exit code 0, this shouldn't ever happen.  Bailing.\n");
  exit(1);
}



/*
 *  Validation function for Invocation
 */
void validate(int argc, char **argv) {
  if(argc == 1)
    fatal(E_GENERIC, "%s%s%s", "Usage: ", argv[0], " /path/to/data/folder");
  if(argc != 2)
    fatal(E_GENERIC, "%s", "You must send exactly 1 argument to this program: the full path to the files to work with.");
  if(strlen(argv[1]) == 0 || strncmp(argv[1], "/", 1) != 0)
    fatal(E_GENERIC, "%s", "You must send a valid path to scan for files (non-recursive).  It should start with: /something");
  // Directory Checking
  DIR *dir = opendir(argv[1]);
  if(!dir)
    fatal(E_IO, "%s%s", "Can't open directory: bad path, isn't a directory, missing permission, etc.: ", argv[1]);
  closedir(dir);
  return;
}



/*
 *  Scan for files in the specified directory.  Caller must provide initial validation.  We fatal if no files found.
 */
void scan_files(char *path, src_file files[], int *file_count) {
  DIR *dir = opendir(path);
  struct dirent *entry;
  FILE *fh = NULL;

  // For simplicity we'll put a trailing slash on path.
  if(strcmp(path + strlen(path) - 1, "/") != 0) {
    char *new_path = malloc(strlen(path) + 1);
    strcpy(new_path, path);
    strcat(new_path, "/");
    path = new_path;
  }

  // Scan (non-recursively)
  if(!dir)
    fatal(E_IO, "%s%s", "scan_files failed to open directory: ", path);
  entry = readdir(dir);
  while(entry != NULL) {
    if(entry->d_type == DT_REG) {
      files[(*file_count)].filespec = malloc(strlen(path) + strlen(entry->d_name) + 1);
      strcpy(files[(*file_count)].filespec, path);
      strcat(files[(*file_count)].filespec, entry->d_name);
      (*file_count)++;
    }
    entry = readdir(dir);
  }
  closedir(dir);

  // Make sure we found at least 1 file, and we have rights to read each file.
  if(*file_count < 1)
    fatal(E_IO, "%s", "Unable to find any files :(");
  for(int i=0; i<(*file_count); i++) {
    fh = fopen(files[i].filespec, "rb");
    if(fh == NULL)
      fatal(E_IO, "%s%s", "Unable to open file for binary reading: ", files[i].filespec);
    fclose(fh);
  }
}



/*
 *  Slurp a file into a large single buffer.  Caller provides pointer, we fill it.
 */
void slurp_file(src_file *src) {
  FILE *fh = fopen(src->filespec, "rb");
  if(fh == NULL)
    fatal(E_IO, "%s%s", "Unable to open file for binary reading: ", src->filespec);

  // Scan file and record length.
  fseek (fh, 0, SEEK_END);
  src->size = ftell (fh);

  // Rewind and slurp the data into a buffer.
  fseek (fh, 0, SEEK_SET);
  src->data = malloc(src->size);
  if(src->data == NULL)
    fatal(E_GENERIC, "%s%s", "Failed to malloc space for file slurp: ", src->filespec);
  if(!fread(src->data, 1, src->size, fh))
    fatal(E_IO, "%s", "Failed to read a file into memory while slurping.");

  fclose(fh);

  // All done.
  return;
}
void unslurp_file(src_file *src) {
  // For now all I think we need to do is free the *data.  The rest is trivial.
  if(src->data) {
    free(src->data);
    src->data = NULL;
  }
}



/*
 *  Print a table entry, header, or etc.
 */
const int fields[16] = {16, 10, 10, 6, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8};
char *hyphens = "----------------------------------------------------------------------------------------------------";
char *blank = "                                                                                                  ";
int to_kib(uint64_t value) {
  return (int)(value / 1024);
}
int ns_to_ms(uint64_t value) {
  return (int)(value / MILLION);
}
void print_separator(char *column, char *fill) {
  // Note: we +2 to complement padding of strings/values.
  printf("%1s%*.*s%1s%*.*s%1s%*.*s%1s%*.*s%1s%*.*s%1s%*.*s%1s%*.*s%1s\n",
    column, fields[0] + 2, fields[0] + 2, fill,
    column, fields[1] + 2, fields[1] + 2, fill,
    column, fields[2] + 2, fields[2] + 2, fill,
    column, fields[3] + 2, fields[3] + 2, fill,
    column, fields[4] + fields[5] + fields[6] + fields[7] + 2, fields[4] + fields[5] + fields[6] + fields[7] + 2, fill,
    column, fields[8] + fields[9] + fields[10] + fields[11] + 2, fields[8] + fields[9] + fields[10] + fields[11] + 2, fill,
    column, fields[12] + fields[13] + fields[14] + fields[15] + 2, fields[12] + fields[13] + fields[14] + fields[15] + 2, fill,
    column
  );
}
void print_header() {
  print_separator("+", hyphens);
  printf("| %-*.*s | %*.*s | %*.*s | %*.*s | %-*.*s | %-*.*s | %-*.*s |\n",
    fields[0], fields[0], "Data File",
    fields[1], fields[1], "KiB (2^10)",
    fields[2], fields[2], "Block Size",
    fields[3], fields[3], "Blocks",
    fields[4] + fields[5] + fields[6] + fields[7], fields[4] + fields[5] + fields[6] + fields[7], "Compression Size (KiB)",
    fields[8] + fields[9] + fields[10] + fields[11], fields[8] + fields[9] + fields[10] + fields[11], "Compression Time (ms)",
    fields[12] + fields[13] + fields[14] + fields[15], fields[12] + fields[13] + fields[14] + fields[15], "Decompression Time (ms)"
  );
  printf("| %*.*s | %*.*s | %*.*s | %*.*s | %*.*s%*.*s%*.*s%*.*s | %*.*s%*.*s%*.*s%*.*s | %*.*s%*.*s%*.*s%*.*s |\n",
    fields[0], fields[0], blank,
    fields[1], fields[1], blank,
    fields[2], fields[2], blank,
    fields[3], fields[3], blank,
    fields[4], fields[4], "Memcpy",
    fields[5], fields[5], "LZ4",
    fields[6], fields[6], "ZLIB",
    fields[7], fields[7], "ZSTD",
    fields[8], fields[8], "Memcpy",
    fields[9], fields[9], "LZ4",
    fields[10], fields[10], "ZLIB",
    fields[11], fields[11], "ZSTD",
    fields[12], fields[12], "Memcpy",
    fields[13], fields[13], "LZ4",
    fields[14], fields[14], "ZLIB",
    fields[15], fields[15], "ZSTD"
  );
  print_separator("+", hyphens);
}
void print_result(result *res) {
  printf("| %-*.*s | %*i | %*i | %*i | %*i%*i%*i%*i | %*i%*i%*i%*i | %*i%*i%*i%*i |\n",
    fields[0], fields[0], basename(res->src->filespec),
    fields[1], to_kib(res->src->size),
    fields[2], res->block_size,
    fields[3], res->blocks,
    fields[4], to_kib(res->src->size),
    fields[5], to_kib(res->lz4_comp_size),
    fields[6], to_kib(res->zlib_comp_size),
    fields[7], to_kib(res->zstd_comp_size),
    fields[8], ns_to_ms(res->memcpy_time),
    fields[9], ns_to_ms(res->lz4_comp_time),
    fields[10], ns_to_ms(res->zlib_comp_time),
    fields[11], ns_to_ms(res->zstd_comp_time),
    fields[12], ns_to_ms(res->memcpy_time),
    fields[13], ns_to_ms(res->lz4_decomp_time),
    fields[14], ns_to_ms(res->zlib_decomp_time),
    fields[15], ns_to_ms(res->zstd_decomp_time)
  );
}



/*
 *  Compression test on a slurped file with a given block size.
 */
void compression_test(src_file *src, int block_size) {
  // Locals
  result res;
  static buffer bufs[MAX_BUFFERS];  // Static to avoid stack overflow.  I'm too lazy to malloc.
  int buffer_count = 0;
  struct timespec start, end;
  int zlib_errors = 0;

  // To avoid repeatedly malloc/free()-ing we'll just over allocate now and reuse.
  buffer_count = src->size / block_size;
  if(src->size % block_size > 0)
    buffer_count++;
  for(int i=0; i<buffer_count; i++) {
    bufs[i].comp_size = 0;
    bufs[i].raw_size = block_size;
    if(i + 1 == buffer_count && src->size % block_size > 0)
      bufs[i].raw_size = src->size % block_size;
    bufs[i].raw = malloc(bufs[i].raw_size);
    bufs[i].compressed = malloc(bufs[i].raw_size + 512);  // Overkill but meh.
    bufs[i].decompressed = malloc(bufs[i].raw_size);
    if(bufs[i].raw == NULL || bufs[i].compressed == NULL || bufs[i].decompressed == NULL)
      fatal(E_GENERIC, "%s", "Failed to allocate memory for buffers.");
  }
  // Copy some result values for the test.
  res.src = src;
  res.block_size = block_size;
  res.blocks = buffer_count;

  // Run the tests.  To reduce the effects of caching-warming we use one compressor at a time.
  // -- Memcpy
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++)
    memcpy(bufs[i].raw, src->data + (i * block_size), bufs[i].raw_size);
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.memcpy_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;

  // -- LZ4
  // Compress Time
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++)
    bufs[i].comp_size = LZ4_compress_default(bufs[i].raw, bufs[i].compressed, bufs[i].raw_size, bufs[i].raw_size);
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.lz4_comp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Compress Size
  res.lz4_comp_size = 0;
  for(int i=0; i<buffer_count; i++)
    res.lz4_comp_size += bufs[i].comp_size;
  // Decompress Time
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++)
    LZ4_decompress_safe(bufs[i].compressed, bufs[i].decompressed, bufs[i].comp_size, bufs[i].raw_size);
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.lz4_decomp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Validation (now that we're not timing anything).
  for(int i=0; i<buffer_count; i++) {
    if(bufs[i].comp_size < 0)
      fatal(E_GENERIC, "%s (id: %d)", "There was a problem with buffer", i);
  }

  // -- ZLIB
  // Compress Time
  clock_gettime(CLOCK_MONOTONIC, &start);
  uLongf max_compressed_size = 0;
  uLongf data_length = 0;
  for(int i=0; i<buffer_count; i++) {
    max_compressed_size = compressBound(bufs[i].raw_size);
    zlib_errors += compress2(bufs[i].compressed, &max_compressed_size, bufs[i].raw, bufs[i].raw_size, ZLIB_LEVEL);
    bufs[i].comp_size = max_compressed_size;
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.zlib_comp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Compress Size
  res.zlib_comp_size = 0;
  for(int i=0; i<buffer_count; i++)
    res.zlib_comp_size += bufs[i].comp_size;
  // Decompress Time
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++) {
    data_length = bufs[i].raw_size;
    zlib_errors += uncompress(bufs[i].decompressed, &data_length, bufs[i].compressed, bufs[i].comp_size);
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.zlib_decomp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Validation (now that we're not timing anything).
  if(zlib_errors > 0)
    fatal(E_GENERIC, "%s (errors: %i)", "Ran into a compression problem with zlib.", zlib_errors);

  // -- ZSTD
  // Compress Time
  //rv = ZSTD_decompress(decompressed_data, buf->data_length, buf->data, buf->comp_length);
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++)
    bufs[i].comp_size = ZSTD_compress(bufs[i].compressed, bufs[i].raw_size + 512, bufs[i].raw, bufs[i].raw_size, ZSTD_LEVEL);
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.zstd_comp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Compress Size
  res.zstd_comp_size = 0;
  for(int i=0; i<buffer_count; i++)
    res.zstd_comp_size += bufs[i].comp_size;
  // Decompress Time
  clock_gettime(CLOCK_MONOTONIC, &start);
  for(int i=0; i<buffer_count; i++)
    ZSTD_decompress(bufs[i].decompressed, bufs[i].raw_size, bufs[i].compressed, bufs[i].comp_size);
  clock_gettime(CLOCK_MONOTONIC, &end);
  res.zstd_decomp_time = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
  // Validation (now that we're not timing anything).
  for(int i=0; i<buffer_count; i++) {
    if(bufs[i].comp_size < 0)
      fatal(E_GENERIC, "%s (id: %d)", "There was a problem with buffer", i);
  }

  // Print results.
  print_result(&res);

  // Clean up and leave.
  for(int i=0; i<buffer_count; i++) {
    free(bufs[i].compressed);
    free(bufs[i].decompressed);
    free(bufs[i].raw);
  }
}



/*
 *  Main function, initialization point for program.
 */
int main(int argc, char **argv) {
  // Locals
  src_file files[MAX_FILES];
  char *path;
  int file_count = 0;

  // 1.  Validate arguments.  Then load files into array (do NOT slurp here).
  validate(argc, argv);
  path = argv[1];
  scan_files(path, files, &file_count);

  // 2.  Main loop to do our testing.
  print_header();
  for(int i=0; i<file_count; i++) {
    slurp_file(&files[i]);
    for(int block_id=0; block_id<BLOCK_COUNT; block_id++) {
      compression_test(&files[i], block_sizes[block_id]);
    }
    print_separator("|", blank);
    unslurp_file(&files[i]);
  }
  print_separator("+", hyphens);

  // All done.
  return 0;
}


