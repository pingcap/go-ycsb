#ifndef STORAGE_ROCKSDB_INCLUDE_C_EXT_H_
#define STORAGE_ROCKSDB_INCLUDE_C_EXT_H_

#pragma once

#ifdef _WIN32
#ifdef ROCKSDB_DLL
#ifdef ROCKSDB_LIBRARY_EXPORTS
#define ROCKSDB_LIBRARY_API __declspec(dllexport)
#else
#define ROCKSDB_LIBRARY_API __declspec(dllimport)
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif
#else
#define ROCKSDB_LIBRARY_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/c.h"

/* Options */

extern ROCKSDB_LIBRARY_API void rocksdb_options_set_max_background_jobs(
    rocksdb_options_t*, int);

/* Block based table options */

extern ROCKSDB_LIBRARY_API void rocksdb_block_based_options_set_block_align(
    rocksdb_block_based_table_options_t* options, unsigned char v);

#ifdef __cplusplus
}  /* end extern "C" */
#endif

#endif  /* STORAGE_ROCKSDB_INCLUDE_C_H_ */