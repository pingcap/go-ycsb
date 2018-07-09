/*
 * Platform.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FLOW_PLATFORM_H
#define FLOW_PLATFORM_H
#pragma once

#if (defined(__linux__) || defined(__APPLE__))
#define __unixish__ 1
#endif

#define FLOW_THREAD_SAFE 0

#include <stdlib.h>

#define FDB_EXIT_SUCCESS 0
#define FDB_EXIT_ERROR 1
#define FDB_EXIT_ABORT 3
#define FDB_EXIT_MAIN_ERROR 10
#define FDB_EXIT_MAIN_EXCEPTION 11
#define FDB_EXIT_NO_MEM 20
#define FDB_EXIT_INIT_SEMAPHORE 21

#ifdef __cplusplus
#define EXTERNC extern "C"

#include <cstdlib>
#include <cstdint>
#include <stdio.h>

#ifdef __unixish__
#include <unistd.h>
#endif

#if !(defined(_WIN32) || defined(__unixish__))
#error Compiling on unknown platform
#endif

#if defined(__linux__) && ((__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) < 40500)
#error GCC 4.5.0 or later required on this platform
#endif

#if defined(_WIN32) && (_MSC_VER < 1600)
#error Visual Studio 2010 required on this platform
#endif

#if defined(__APPLE__) && (!((__clang__ == 1) || ((__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__) > 40800)))
#error Either Clang or GCC 4.8.0 or later required on this platform
#endif

#if (__clang__ == 1)
#define DISABLE_ZERO_DIVISION_FLAG _Pragma("GCC diagnostic ignored \"-Wdivision-by-zero\"")
#elif defined(_MSC_VER)
#define DISABLE_ZERO_DIVISION_FLAG __pragma("GCC diagnostic ignored \"-Wdiv-by-zero\"")
#else
#define DISABLE_ZERO_DIVISION_FLAG _Pragma("GCC diagnostic ignored \"-Wdiv-by-zero\"")
#endif

/*
 * Thread-local storage (but keep in mind any platform-specific
 * restrictions on where this is valid and/or ignored).
 *
 * http://en.wikipedia.org/wiki/Thread-local_storage
 *
 * SOMEDAY: Intel C++ compiler uses g++ syntax on Linux and MSC syntax
 * on Windows.
 */
#if defined(__GNUG__)
#define thread_local __thread
#elif defined(_MSC_VER)
#define thread_local __declspec(thread)
#else
#error Missing thread local storage
#endif

#if defined(__GNUG__)
#define force_inline inline __attribute__((__always_inline__))
#elif defined(_MSC_VER)
#define force_inline __forceinline
#else
#error Missing force inline
#endif

/*
 * Visual Studio 2005 and beyond allow virtual and sealed while
 * targetting native code, and we get better error messages at compile
 * time with it where appropriate. Not supported with any other
 * compiler.
 */
#if _MSC_VER < 1400
#define sealed
#define override
#endif

/*
 * Visual Studio (.NET 2003 and beyond) has an __assume compiler
 * intrinsic to hint to the compiler that a given condition is true
 * and will remain true until the expression is altered. This can be
 * emulated on GCC and ignored elsewhere.
 *
 * http://en.chys.info/2010/07/counterpart-of-assume-in-gcc/
 */
#ifndef _MSC_VER
#if defined(__GNUG__)
#define __assume(cond) do { if (!(cond)) __builtin_unreachable(); } while (0)
#else
#define __assume(cond)
#endif
#endif

#ifdef __unixish__
#include <pthread.h>
#define CRITICAL_SECTION pthread_mutex_t
#define InitializeCriticalSection(m) \
do { \
	pthread_mutexattr_t mta; \
	pthread_mutexattr_init(&mta); \
	pthread_mutexattr_settype(&mta, PTHREAD_MUTEX_RECURSIVE); \
	pthread_mutex_init(m, &mta); \
	pthread_mutexattr_destroy(&mta); \
	} while (0)
#define DeleteCriticalSection(m) pthread_mutex_destroy(m)
#define EnterCriticalSection(m) pthread_mutex_lock(m)
#define LeaveCriticalSection(m) pthread_mutex_unlock(m)
#endif

#if (defined(__GNUG__))
#include <memory>
#include <functional>
#endif

// fake<T>() is for use in decltype expressions only - there is no implementation
template <class T> T fake();

// g++ requires that non-dependent names have to be looked up at
// template definition, which makes circular dependencies a royal
// pain. (For whatever it's worth, g++ appears to be adhering to spec
// here.) Fixing this properly requires quite a bit of reordering
// and/or splitting things into multiple files, but Scherer pointed
// out that it's simple to force a name to be dependent, which is what
// we'll do for now.
template <class Ignore, class T>
inline static T& makeDependent(T& value) { return value; }

#include <string>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#define THREAD_FUNC static void __cdecl
#define THREAD_FUNC_RETURN void
#define THREAD_HANDLE void *
THREAD_HANDLE startThread(void (func) (void *), void *arg);
#define THREAD_RETURN return
#elif defined(__unixish__)
#define THREAD_FUNC static void *
#define THREAD_FUNC_RETURN void *
#define THREAD_HANDLE pthread_t
THREAD_HANDLE startThread(void *(func) (void *), void *arg);
#define THREAD_RETURN return NULL
#else
#error How do I start a new thread on this platform?
#endif

#if defined(_WIN32)
#define DYNAMIC_LIB_EXT ".dll"
#elif defined(__linux)
#define DYNAMIC_LIB_EXT ".so"
#elif defined(__APPLE__)
#define DYNAMIC_LIB_EXT ".dylib"
#else
#error Port me
#endif

#if defined(_WIN32)
#define ENV_VAR_PATH_SEPARATOR ';'
#elif defined(__unixish__)
#define ENV_VAR_PATH_SEPARATOR ':'
#else
#error Port me
#endif

void waitThread(THREAD_HANDLE thread);

// Linux-only for now.  Set thread priority "low"
void deprioritizeThread();

#define DEBUG_DETERMINISM 0

std::string removeWhitespace(const std::string &t);

struct SystemStatistics {
	bool initialized;
	double elapsed;
	double processCPUSeconds, mainThreadCPUSeconds;
	uint64_t processMemory;
	uint64_t processResidentMemory;
	uint64_t processDiskTotalBytes;
	uint64_t processDiskFreeBytes;
	double processDiskQueueDepth;
	double processDiskIdleSeconds;
	double processDiskRead;
	double processDiskWrite;
	uint64_t processDiskReadCount;
	uint64_t processDiskWriteCount;
	double processDiskWriteSectors;
	double processDiskReadSectors;
	double machineMegabitsSent;
	double machineMegabitsReceived;
	uint64_t machineOutSegs;
	uint64_t machineRetransSegs;
	double machineCPUSeconds;
	int64_t machineTotalRAM;
	int64_t machineCommittedRAM;
	int64_t machineAvailableRAM;

	SystemStatistics() : initialized(false), elapsed(0), processCPUSeconds(0), mainThreadCPUSeconds(0), processMemory(0),
		processResidentMemory(0), processDiskTotalBytes(0), processDiskFreeBytes(0), processDiskQueueDepth(0), processDiskIdleSeconds(0), processDiskRead(0), processDiskWrite(0),
		processDiskReadCount(0), processDiskWriteCount(0), processDiskWriteSectors(0), processDiskReadSectors(0), machineMegabitsSent(0), machineMegabitsReceived(0), machineOutSegs(0),
		machineRetransSegs(0), machineCPUSeconds(0), machineTotalRAM(0), machineCommittedRAM(0), machineAvailableRAM(0) {}
};

struct SystemStatisticsState;

SystemStatistics getSystemStatistics(std::string dataFolder, uint32_t ip, SystemStatisticsState **statState);

double getProcessorTimeThread();

double getProcessorTimeProcess();

uint64_t getMemoryUsage();

uint64_t getResidentMemoryUsage();

struct MachineRAMInfo {
	int64_t total;
	int64_t committed;
	int64_t available;
};

void getMachineRAMInfo(MachineRAMInfo& memInfo);

void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total);

void getNetworkTraffic(uint64_t& bytesSent, uint64_t& bytesReceived, uint64_t& outSegs,
					   uint64_t& retransSegs);

void getDiskStatistics(std::string const& directory, uint64_t& currentIOs, uint64_t& busyTicks, uint64_t& reads, uint64_t& writes, uint64_t& writeSectors);

void getMachineLoad(uint64_t& idleTime, uint64_t& totalTime);

double timer();  // Returns the system real time clock with high precision.  May jump around when system time is adjusted!
double timer_monotonic();  // Returns a high precision monotonic clock which is adjusted to be kind of similar to timer() at startup, but might not be a globally accurate time.
uint64_t timer_int(); // Return timer as uint64_t

void getLocalTime(const time_t *timep, struct tm *result);

void setMemoryQuota(size_t limit);

void *allocate(size_t length, bool allowLargePages);

void setAffinity(int proc);

void threadSleep( double seconds );

void threadYield();  // Attempt to yield to other processes or threads

// Returns true iff the file exists
bool fileExists(std::string const& filename);

// Returns size of file in bytes
int64_t fileSize(std::string const& filename);

// Returns true if file is deleted, false if it was not found, throws platform_error() otherwise
// Consider using IAsyncFileSystem::filesystem()->deleteFile() instead, especially if you need durability!
bool deleteFile( std::string const& filename );

// Renames the given file.  Does not fsync the directory.
void renameFile( std::string const& fromPath, std::string const& toPath );

// Atomically replaces the contents of the specified file.
void atomicReplace( std::string const& path, std::string const& content );

// Read a file into memory
std::string readFileBytes( std::string const& filename, int maxSize );

// Write data buffer into file
void writeFileBytes(std::string const& filename, const char* data, size_t count);

// Write text into file
void writeFile(std::string const& filename, std::string const& content);

std::string joinPath( std::string const& directory, std::string const& filename );

// Returns an absolute path canonicalized to use only CANONICAL_PATH_SEPARATOR
std::string abspath( std::string const& filename );

// Returns the portion of the path following the last path separator (e.g. the filename or directory name)
std::string basename( std::string const& filename );

// Returns the parent directory of the given file or directory
std::string parentDirectory( std::string const& filename );

// Returns the home directory of the current user
std::string getUserHomeDirectory();

namespace platform {

// Returns true if directory was created, false if it existed, throws platform_error() otherwise
bool createDirectory( std::string const& directory );

// e.g. extension==".fdb", returns filenames relative to directory
std::vector<std::string> listFiles( std::string const& directory, std::string const& extension = "");

// returns directory names relative to directory
std::vector<std::string> listDirectories( std::string const& directory );

void findFilesRecursively(std::string path, std::vector<std::string> &out);

// Tag the given file as "temporary", i.e. not really needing commits to disk
void makeTemporary( const char* filename );

// Logs an out of memory error and exits the program
void outOfMemory();

int getRandomSeed();

bool getEnvironmentVar(const char* name, std::string& value);
int setEnvironmentVar(const char *name, const char *value, int overwrite);

std::string getWorkingDirectory();

// Returns the ... something something figure out plugin locations
std::string getDefaultPluginPath( const char* plugin_name );

void *getImageOffset();

// Places the frame pointers in a string formatted as parameters for addr2line.
size_t raw_backtrace(void** addresses, int maxStackDepth);
std::string get_backtrace();
std::string format_backtrace(void **addresses, int numAddresses);

}; // namespace platform

#ifdef __linux__
typedef struct {
	double timestamp;
	size_t length;
	void* frames[];
} ProfilingSample;

dev_t getDeviceId(std::string path);
#endif

#ifdef __linux__
#include <x86intrin.h>
#include <features.h>
#include <sys/stat.h>
#endif

// Version of CLang bundled with XCode doesn't yet include ia32intrin.h.
#ifdef __APPLE__
#if !(__has_builtin(__rdtsc))
inline static uint64_t __rdtsc() {
	uint64_t lo, hi;
	asm( "rdtsc" : "=a" (lo), "=d" (hi) );
	return( lo | (hi << 32) );
}
#endif
#endif

#ifdef _WIN32
#include <intrin.h>
inline static int32_t interlockedIncrement(volatile int32_t *a) { return _InterlockedIncrement((long*)a); }
inline static int64_t interlockedIncrement64(volatile int64_t *a) { return _InterlockedIncrement64(a); }
inline static int32_t interlockedDecrement(volatile int32_t *a) { return _InterlockedDecrement((long*)a); }
inline static int64_t interlockedDecrement64(volatile int64_t *a) { return _InterlockedDecrement64(a); }
inline static int32_t interlockedCompareExchange(volatile int32_t *a, int32_t b, int32_t c) { return _InterlockedCompareExchange((long*)a, (long)b, (long)c); }
inline static int64_t interlockedExchangeAdd64(volatile int64_t *a, int64_t b) { return _InterlockedExchangeAdd64(a, b); }
inline static int64_t interlockedExchange64(volatile int64_t *a, int64_t b) { return _InterlockedExchange64(a, b); }
inline static int64_t interlockedOr64(volatile int64_t *a, int64_t b) { return _InterlockedOr64(a, b); }
#elif defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8)
#include <xmmintrin.h>
inline static int32_t interlockedIncrement(volatile int32_t *a) { return __sync_add_and_fetch(a, 1); }
inline static int64_t interlockedIncrement64(volatile int64_t *a) { return __sync_add_and_fetch(a, 1); }
inline static int32_t interlockedDecrement(volatile int32_t *a) { return __sync_add_and_fetch(a, -1); }
inline static int64_t interlockedDecrement64(volatile int64_t *a) { return __sync_add_and_fetch(a, -1); }
inline static int32_t interlockedCompareExchange(volatile int32_t *a, int32_t b, int32_t c) { return __sync_val_compare_and_swap(a, c, b); }
inline static int64_t interlockedExchangeAdd64(volatile int64_t *a, int64_t b) { return __sync_fetch_and_add(a, b); }
inline static int64_t interlockedExchange64(volatile int64_t *a, int64_t b) {
	__sync_synchronize();
	return __sync_lock_test_and_set(a, b);
}
inline static int64_t interlockedOr64(volatile int64_t *a, int64_t b) { return __sync_fetch_and_or(a, b); }
#else
#error No implementation of atomic instructions
#endif

template <class T> inline static T* interlockedExchangePtr(T*volatile*a, T*b) { static_assert(sizeof(T*)==sizeof(int64_t),"Port me!"); return (T*)interlockedExchange64((volatile int64_t*)a, (int64_t)b); }

#if FLOW_THREAD_SAFE
#define thread_volatile volatile
inline static int64_t flowInterlockedExchangeAdd64( volatile int64_t* p, int64_t a ) { return interlockedExchangeAdd64(p, a); }
inline static int64_t flowInterlockedIncrement64( volatile int64_t* p ) { return interlockedIncrement64(p); }
inline static int64_t flowInterlockedDecrement64( volatile int64_t* p ) { return interlockedDecrement64(p); }
inline static int64_t flowInterlockedExchange64( volatile int64_t* p, int64_t a ) { return interlockedExchange64(p, a); }
inline static int64_t flowInterlockedOr64( volatile int64_t* p, int64_t a ) { return interlockedOr64(p, a); }
inline static int64_t flowInterlockedAnd64( volatile int64_t* p, int64_t a ) { return interlockedAnd64(p, a); }
#else
#define thread_volatile
inline static int64_t flowInterlockedExchangeAdd64( int64_t* p, int64_t a ) { auto old=*p; *p+=a; return old; }
inline static int64_t flowInterlockedIncrement64( int64_t* p ) { return ++*p; }
inline static int64_t flowInterlockedDecrement64( int64_t* p ) { return --*p; }
inline static int64_t flowInterlockedExchange64( int64_t* p, int64_t a ) { auto old=*p; *p=a; return old; }
inline static int64_t flowInterlockedOr64( int64_t* p, int64_t a ) { auto old=*p; *p |= a; return old; }
inline static int64_t flowInterlockedAnd64( int64_t* p, int64_t a ) { auto old=*p; *p &= a; return old; }
#endif

// We only run on little-endian system, so conversion to/from bigEndian64 is always a byte swap
#ifdef _MSC_VER
#define bigEndian16(value) uint16_t(_byteswap_ushort(value))
#define bigEndian32(value) uint32_t(_byteswap_ulong(value))
#define bigEndian64(value) uint64_t(_byteswap_uint64(value))
#elif __GNUG__
#define bigEndian16(value) uint16_t((value>>8)|(value<<8))
#define bigEndian32(value) uint32_t(__builtin_bswap32(value))
#define bigEndian64(value) uint64_t(__builtin_bswap64(value))
#else
#error Missing byte swap methods
#endif

#define littleEndian16(value) value
#define littleEndian32(value) value
#define littleEndian64(value) value

#if defined(_WIN32)
inline static void flushOutputStreams() { _flushall(); }
#elif defined(__unixish__)
inline static void flushOutputStreams() { fflush(NULL); }
#else
#error Missing flush output stream
#endif


#if defined(_MSC_VER)
#define DLLEXPORT __declspec(dllexport)
#elif defined(__GNUG__)
#define DLLEXPORT __attribute__ ((visibility ("default")))
#else
#error Missing symbol export
#endif

#define crashAndDie() (*(volatile int*)0 = 0)


#if defined(__GNUG__)
#define DEFAULT_CONSTRUCTORS(X) \
	X( X const& rhs ) = default; \
	X& operator=( X const& rhs ) = default;
#else
#define DEFAULT_CONSTRUCTORS(X)
#endif


#if defined(_WIN32)
#define strtoull(nptr, endptr, base) _strtoui64(nptr, endptr, base)
#endif

#if defined(_MSC_VER)
inline static void* aligned_alloc(size_t alignment, size_t size) { return _aligned_malloc(size, alignment); }
inline static void aligned_free(void* ptr) { _aligned_free(ptr); }
#elif defined(__linux__)
#include <malloc.h>
inline static void aligned_free(void* ptr) { free(ptr); }
#if (!defined(_ISOC11_SOURCE)) // old libc versions
inline static void* aligned_alloc(size_t alignment, size_t size) { return memalign(alignment, size); }
#endif
#elif defined(__APPLE__)
#include <cstdlib>
inline static void* aligned_alloc(size_t alignment, size_t size) {
	void* ptr = nullptr;
	posix_memalign(&ptr, alignment, size);
	return ptr;
}
inline static void aligned_free(void* ptr) { free(ptr); }
#endif

// lib_path may be a relative or absolute path or a name to be
// resolved by whatever linker is hanging around on this system
bool isLibraryLoaded(const char* lib_path);
void* loadLibrary(const char* lib_path);
void* loadFunction(void* lib, const char* func_name);

// MSVC not support noexcept yet
#ifndef __GNUG__
#ifndef VS14
#define noexcept(enabled)
#endif
#endif

#else
#define EXTERNC
#endif // __cplusplus

// Logs a critical error message and exits the program
EXTERNC void criticalError(int exitCode, const char *type, const char *message);
EXTERNC void flushAndExit(int exitCode);

// Initilization code that's run at the beginning of every entry point (except fdbmonitor)
void platformInit();

void registerCrashHandler();
void setupSlowTaskProfiler();
EXTERNC void setProfilingEnabled(int enabled);

// Use _exit() or criticalError(), not exit()
#define CALLS_TO_EXIT_ARE_FORBIDDEN_BY_POLICY() [====]
#define exit CALLS_TO_EXIT_ARE_FORBIDDEN_BY_POLICY(0)

#if defined(FDB_CLEAN_BUILD) && !( defined(NDEBUG) && !defined(_DEBUG) && !defined(SQLITE_DEBUG) )
#error Clean builds must define NDEBUG, and not define various debug macros
#endif

#endif /* FLOW_PLATFORM_H */
