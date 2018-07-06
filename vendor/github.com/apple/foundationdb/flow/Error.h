/*
 * Error.h
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

#ifndef FLOW_ERROR_H
#define FLOW_ERROR_H
#pragma once

#include <exception>
#include <map>
#include <boost/preprocessor/assert_msg.hpp>
#include <boost/preprocessor/facilities/is_empty.hpp>
#include <boost/preprocessor/control/if.hpp>
#include "Platform.h"
#include "Knobs.h"

enum { invalid_error_code = 0xffff };

class ErrorCodeTable : public std::map<int, std::pair<const char*, const char*>> {
public:
	ErrorCodeTable();
	void addCode(int code, const char *name, const char *description);
};

class Error {
public:
	int code() const { return error_code; }
	const char* name() const;
	const char* what() const;
	bool isInjectedFault() const { return flags & FLAG_INJECTED_FAULT; }  // Use as little as possible, so injected faults effectively test real faults!
	bool isValid() const { return error_code != invalid_error_code; }

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & error_code;
	}

	Error() : error_code(invalid_error_code), flags(0) {}
	explicit Error(int error_code);

	static void init();
	static std::map<int, int>& errorCounts();
	static ErrorCodeTable& errorCodeTable();
	static Error fromCode(int error_code) { Error e; e.error_code = error_code; return e; }  // Doesn't change errorCounts
	static Error fromUnvalidatedCode(int error_code);  // Converts codes that are outside the legal range (but not necessarily individually unknown error codes) to unknown_error()

	Error asInjectedFault() const;  // Returns an error with the same code() as this but isInjectedFault() is true
private:
	uint16_t error_code;
	uint16_t flags;

	enum Flags { FLAG_INJECTED_FAULT=1 };
};

#undef ERROR
#define ERROR(name, number, description) inline Error name() { return Error( number ); }; enum { error_code_##name = number };
#include "error_definitions.h"

//actor_cancelled has been renamed
inline Error actor_cancelled() { return Error( error_code_operation_cancelled ); }
enum { error_code_actor_cancelled = error_code_operation_cancelled };

extern Error internal_error_impl( const char* file, int line );
#define internal_error() internal_error_impl( __FILE__, __LINE__ )

extern bool isAssertDisabled( int line );
//#define ASSERT( condition ) ((void)0)
#define ASSERT( condition ) if (!((condition) || isAssertDisabled(__LINE__))) { throw internal_error(); }
#define ASSERT_ABORT( condition ) if (!((condition) || isAssertDisabled(__LINE__))) { internal_error(); abort(); }   // For use in destructors, where throwing exceptions is extremely dangerous
#define UNSTOPPABLE_ASSERT( condition ) if (!(condition)) { throw internal_error(); }
#define UNREACHABLE() { throw internal_error(); }

// ASSERT_WE_THINK() is to be used for assertions that we want to validate in testing, but which are judged too
// risky to evaluate at runtime, because the code should work even if they are false and throwing internal_error() would
// result in a bug.  Don't use it for assertions that are *expensive*; look at EXPENSIVE_VALIDATION.
#define ASSERT_WE_THINK( condition ) ASSERT( !g_network->isSimulated() || condition )

#define ABORT_ON_ERROR( code_to_run ) \
	try { code_to_run; } \
	catch(Error &e) { criticalError(FDB_EXIT_ABORT, "AbortOnError", e.what()); } \
	catch(...) { criticalError(FDB_EXIT_ABORT, "AbortOnError", "Aborted due to unknown error"); }

#ifdef FDB_CLEAN_BUILD
#  define NOT_IN_CLEAN BOOST_STATIC_ASSERT_MSG(0, "This code can not be enabled in a clean build.");
#else
#  define NOT_IN_CLEAN
#endif

#define FDB_EXPAND(...) __VA_ARGS__
#define FDB_STRINGIZE(...) BOOST_PP_IIF(BOOST_PP_IS_EMPTY(FDB_EXPAND(__VA_ARGS__)), "", #__VA_ARGS__)
#define ENABLED(...) BOOST_PP_IIF(BOOST_PP_IS_EMPTY(FDB_EXPAND(__VA_ARGS__)), 1, BOOST_PP_ASSERT_MSG(0, FDB_STRINGIZE(__VA_ARGS__)))
#define DISABLED(...) BOOST_PP_IIF(BOOST_PP_NOT(BOOST_PP_IS_EMPTY(FDB_EXPAND(__VA_ARGS__))), 1, BOOST_PP_ASSERT_MSG(0, FDB_STRINGIZE(__VA_ARGS__)))

/* Windows compilers won't allow the syntax of:
	 #if 0 && ENABLED(x)
   So these macros replicate that, you instead do:
	 #if CENABLED(0, x)
 */
#define CENABLED(x,y) BOOST_PP_IF(x, ENABLED(y), 0)
#define CDISABLED(x,y) BOOST_PP_IF(x, DISABLED(y), 0)

#endif
