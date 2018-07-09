/*
 * genericactors.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_GENERICACTORS_ACTOR_G_H)
	#define FLOW_GENERICACTORS_ACTOR_G_H
	#include "genericactors.actor.g.h"
#elif !defined(GENERICACTORS_ACTOR_H)
	#define GENERICACTORS_ACTOR_H

#include <list>

#include "actorcompiler.h"
#include "Knobs.h"
#pragma warning( disable: 4355 )	// 'this' : used in base member initializer list

ACTOR template<class T, class X>
Future<T> traceAfter(Future<T> what, const char* type, const char* key, X value, bool traceErrors = false)
{
	try {
		T val = wait(what);
		TraceEvent(type).detail(key, value);
		return val;
	} catch( Error &e ) {
		if(traceErrors) TraceEvent(type).detail(key, value).error(e,true);
		throw;
	}
}

ACTOR template<class T, class X>
Future<T> traceAfterCall(Future<T> what, const char* type, const char* key, X func, bool traceErrors = false)
{
	try {
		state T val = wait(what);
		try {
			TraceEvent(type).detail(key, func(val));
		} catch( Error &e ) {
			TraceEvent(SevError, "TraceAfterCallError").error(e);
		}
		return val;
	} catch( Error &e ) {
		if(traceErrors) TraceEvent(type).error(e,true);
		throw;
	}
}

ACTOR template <class T>
Future<Optional<T>> stopAfter( Future<T> what ) {
	state Optional<T> ret = T();
	try {
		T _ = wait(what);
		ret = Optional<T>(_);
	} catch (Error& e) {
		bool ok = e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete || e.code() == error_code_actor_cancelled;
		TraceEvent(ok ? SevInfo : SevError, "StopAfterError").error(e);
		if(!ok) {
			fprintf(stderr, "Fatal Error: %s\n", e.what());
			ret = Optional<T>();
		}
	}
	g_network->stop();
	return ret;
}

template <class T>
T sorted(T range) {
	std::sort(range.begin(), range.end());
	return range;
}

template <class T>
inline std::vector<T>& operator , (std::vector<T>& v, T a) {
	v.push_back(a);
	return v;
}

template <class T>
inline std::vector<T>& operator , (std::vector<T> && v, T a) {
	return (const_cast<std::vector<T>&>(v), a);
}

template <class T>
ErrorOr<T> errorOr( T t ) {
	return ErrorOr<T>(t);
}

ACTOR template <class T>
Future<ErrorOr<T>> errorOr( Future<T> f ) {
	try {
		T t = wait(f);
		return ErrorOr<T>(t);
	} catch (Error& e) {
		return ErrorOr<T>(e);
	}
}

ACTOR template <class T>
Future<T> throwErrorOr( Future<ErrorOr<T>> f ) {
	ErrorOr<T> t = wait(f);
	if(t.isError()) throw t.getError();
	return t.get();
}

ACTOR template <class T>
Future<T> transformErrors( Future<T> f, Error err ) {
	try {
		T t = wait( f );
		return t;
	} catch( Error &e ) {
		if( e.code() == error_code_actor_cancelled )
			throw e;
		throw err;
	}
}

// Note that the RequestStream<T> version of forwardPromise doesn't exist, because what to do with errors?

ACTOR template <class T>
void forwardEvent( Event* ev, Future<T> input ) {
	try {
		T value = wait(input);
	} catch (Error&) {
	}
	ev->set();
}

ACTOR template <class T>
void forwardEvent( Event* ev, T* t, Error* err, FutureStream<T> input ) {
	try {
		T value = waitNext(input);
		*t = std::move(value);
		ev->set();
	} catch (Error& e) {
		*err = e;
		ev->set();
	}
}

ACTOR template <class T>
Future<Void> waitForAllReady( std::vector<Future<T>> results ) {
	state int i = 0;
	loop {
		if (i == results.size()) return Void();
		try {
			T r = wait( results[i] );
		} catch (...) {
		}
		i++;
	}
}

ACTOR template <class T>
Future<T> timeout( Future<T> what, double time, T timedoutValue, int taskID = TaskDefaultDelay ) {
	Future<Void> end = delay( time, taskID );
	choose {
		when( T t = wait( what ) ) { return t; }
		when( Void _ = wait( end ) ) { return timedoutValue; }
	}
}

ACTOR template <class T>
Future<Optional<T>> timeout( Future<T> what, double time ) {
	Future<Void> end = delay( time );
	choose {
		when( T t = wait( what ) ) { return t; }
		when( Void _ = wait( end ) ) { return Optional<T>(); }
	}
}

ACTOR template <class T>
Future<T> timeoutError( Future<T> what, double time, int taskID = TaskDefaultDelay ) {
	Future<Void> end = delay( time, taskID );
	choose {
		when( T t = wait( what ) ) { return t; }
		when( Void _ = wait( end ) ) { throw timed_out(); }
	}
}

ACTOR template <class T>
Future<T> delayed( Future<T> what, double time = 0.0 ) {
	try {
		state T t = wait( what );
		Void _ = wait( delay(time) );
		return t;
	} catch( Error &e ) {
		state Error err = e;
		Void _ = wait( delay(time) );
		throw err;
	}
}

ACTOR template<class Func>
Future<Void> recurring( Func what, double interval, int taskID = TaskDefaultDelay ) {
	loop choose {
		when ( Void _ = wait( delay( interval, taskID ) ) ) { what(); }
	}
}

ACTOR template<class Func>
Future<Void> trigger( Func what, Future<Void> signal ) {
	Void _ = wait( signal );
	what();
	return Void();
}

ACTOR template<class Func>
Future<Void> triggerOnError( Func what, Future<Void> signal ) {
	try {
		Void _ = wait( signal );
	}
	catch(Error &e) {
		what();
	}

	return Void();
}

//Waits for a future to complete and cannot be cancelled
//Most situations will use the overload below, which does not require a promise
ACTOR template<class T>
void uncancellable(Future<T> what, Promise<T> result)
{
	try {
		T val = wait(what);
		result.send(val);
	} catch( Error &e ) {
		result.sendError(e);
	}
}

//Waits for a future to complete and cannot be cancelled
ACTOR template<class T>
Future<T> uncancellable(Future<T> what)
{
	Promise<T> resultPromise;
	Future<T> result = resultPromise.getFuture();

	uncancellable(what, resultPromise);
	T val = wait(result);

	return val;
}

//Holds onto an object until a future either completes or is cancelled
//Used to prevent the object from being reclaimed
//
// NOTE: the order of the arguments is important. The arguments will be destructed in
// reverse order, and we need the object to be destructed last.
ACTOR template<class T, class X>
Future<T> holdWhile(X object, Future<T> what)
{
	T val = wait(what);
	return val;
}

ACTOR template<class T, class X>
Future<Void> holdWhileVoid(X object, Future<T> what)
{
	T val = wait(what);
	return Void();
}

template<class T>
Future<Void> store(Future<T> what, T &out) {
	return map(what, [&out](T const &v) { out = v; return Void(); });
}

//Waits for a future to be ready, and then applies an asynchronous function to it.
ACTOR template<class T, class F, class U = decltype( fake<F>()(fake<T>()).getValue() )>
Future<U> mapAsync(Future<T> what, F actorFunc)
{
	T val = wait(what);
	U ret = wait(actorFunc(val));
	return ret;
}

//maps a vector of futures with an asynchronous function
template<class T, class F>
std::vector<Future<decltype(actorFunc(T()).getValue())>> mapAsync(std::vector<Future<T>> const& what, F const& actorFunc)
{
	std::vector<typename std::result_of<F(T)>::type> ret;
	for(auto f : what)
		ret.push_back(mapAsync( f, actorFunc ));
	return ret;
}

//maps a stream with an asynchronous function
ACTOR template<class T, class F, class U = decltype( fake<F>()(fake<T>()).getValue() )>
Future<Void> mapAsync( FutureStream<T> input, F actorFunc, PromiseStream<U> output ) {
	state Deque<Future<U>> futures;

	loop {
		try {
			choose {
				when( T nextInput = waitNext( input ) ) {
					futures.push_back( actorFunc(nextInput) );
				}
				when( U nextOutput = wait( futures.size() == 0 ? Never() : futures.front() ) ) {
					output.send( nextOutput );
					futures.pop_front();
				}

			}
		}
		catch ( Error& e ) {
			if( e.code() == error_code_end_of_stream ) {
				break;
			}
			else {
				output.sendError( e );
				throw e;
			}
		}
	}

	while(futures.size()) {
		U nextOutput = wait( futures.front() );
		output.send( nextOutput );
		futures.pop_front();
	}

	output.sendError(end_of_stream());

	return Void();

}

//Waits for a future to be ready, and then applies a function to it.
ACTOR template<class T, class F>
Future<typename std::result_of<F(T)>::type> map(Future<T> what, F func)
{
	T val = wait(what);
	return func(val);
}

//maps a vector of futures
template<class T, class F>
std::vector<Future<typename std::result_of<F(T)>>> map(std::vector<Future<T>> const& what, F const& func)
{
	std::vector<Future<typename std::result_of<F(T)>>> ret;
	for(auto f : what)
		ret.push_back(map( f, func ));
	return ret;
}

//maps a stream
ACTOR template<class T, class F>
Future<Void> map( FutureStream<T> input, F func, PromiseStream<typename std::result_of<F(T)>> output )
{
	loop {
		try {
			T nextInput = waitNext( input );
			output.send(func(nextInput));
		}
		catch ( Error& e ) {
			if( e.code() == error_code_end_of_stream ) {
				break;
			}
			else
				throw;
		}
	}

	output.sendError(end_of_stream());

	return Void();
}

//Returns if the future returns true, otherwise waits forever.
ACTOR static Future<Void> returnIfTrue( Future<bool> f )
{
	bool b = wait( f );
	if ( b ) {
		return Void();
	}
	Void _ = wait( Never() );
	throw internal_error();
}

//Returns if the future, when waited on and then evaluated with the predicate, returns true, otherwise waits forever
template<class T, class F>
Future<Void> returnIfTrue( Future<T> what, F pred)
{
	return returnIfTrue( map( what, pred ) );
}

//filters a stream
ACTOR template<class T, class F>
Future<Void> filter( FutureStream<T> input, F pred, PromiseStream<T> output )
{
	loop {
		try {
			T nextInput = waitNext( input );
			if(func(nextInput))
				output.send(nextInput);
		}
		catch ( Error& e ) {
			if( e.code() == error_code_end_of_stream ) {
				break;
			}
			else
				throw;
		}
	}

	output.sendError(end_of_stream());

	return Void();
}

//filters a stream asynchronously
ACTOR template<class T, class F>
Future<Void> asyncFilter( FutureStream<T> input, F actorPred, PromiseStream<T> output )
{
	state Deque<std::pair<T, Future<bool>>> futures;
	state std::pair<T, Future<bool>> p;

	loop {
		try {
			choose {
				when ( T nextInput = waitNext(input) ) {
					futures.push_back( std::pair<T, Future<bool>>(nextInput, actorPred(nextInput)) );
				}
				when ( bool pass = wait( futures.size() == 0 ? Never() : futures.front().second ) ) {
					if(pass) output.send(futures.front().first);
					futures.pop_front();
				}
			}
		}
		catch ( Error& e ) {
			if( e.code() == error_code_end_of_stream ) {
				break;
			}
			else {
				throw e;
			}
		}
	}

	while(futures.size()) {
		p = futures.front();
		bool pass = wait( p.second );
		if(pass) output.send(p.first);
		futures.pop_front();
	}

	output.sendError(end_of_stream());

	return Void();
}

template <class T>
struct WorkerCache {
	// SOMEDAY: Would we do better to use "unreliable" (at most once) transport for the initialize requests and get rid of this?
	// It doesn't provide true at most once behavior because things are removed from the cache after they have terminated.
	bool exists( UID id ) {
		return id_interface.count( id ) != 0;
	}
	void set( UID id, const Future<T>& onReady ) {
		ASSERT( !exists( id ) );
		id_interface[ id ] = onReady;
	}
	Future<T> get( UID id ) {
		ASSERT( exists( id ) );
		return id_interface[ id ];
	}

	Future<Void> removeOnReady( UID id, Future<Void> const& ready ) {
		return removeOnReady( this, id, ready );
	}
private:
	ACTOR static Future<Void> removeOnReady( WorkerCache* self, UID id, Future<Void> ready ) {
		try {
			Void _ = wait(ready);
			self->id_interface.erase(id);
			return Void();
		} catch ( Error &e ) {
			self->id_interface.erase(id);
			throw;
		}
	}

	std::map<UID, Future<T>> id_interface;
};

template <class K, class V>
class AsyncMap : NonCopyable {
public:
	// Represents a complete function from keys to values (K -> V)
	// All values not explicitly inserted map to V()
	// If this isn't appropriate, use V=Optional<X>

	AsyncMap() : defaultValue(), destructing(false) {}

	virtual ~AsyncMap() {
		destructing = true;
		items.clear();
	}

	void set( K const& k, V const& v ) {
		auto& i = items[k];
		if (i.value != v)
			setUnconditional(k,v,i);
	}
	void setUnconditional( K const& k, V const& v ) {
		setUnconditional(k,v,items[k]);
	}
	void triggerAll() {
		std::vector<Promise<Void>> ps;
		for(auto it = items.begin(); it != items.end(); ++it){
			ps.resize(ps.size()+1);
			ps.back().swap( it->second.change );
		}
		std::vector<Promise<Void>> noDestroy = ps;  // See explanation of noDestroy in setUnconditional()
		for(auto p=ps.begin(); p!=ps.end(); ++p)
			p->send(Void());
	}
	void triggerRange( K const& begin, K const& end ) {
		std::vector<Promise<Void>> ps;
		for(auto it = items.lower_bound(begin); it != items.end() && it->first < end; ++it){
			ps.resize(ps.size()+1);
			ps.back().swap( it->second.change );
		}
		std::vector<Promise<Void>> noDestroy = ps;  // See explanation of noDestroy in setUnconditional()
		for(auto p=ps.begin(); p!=ps.end(); ++p)
			p->send(Void());
	}
	void trigger( K const& key ) {
		if( items.count(key) != 0 ) {
			auto& i = items[key];
			Promise<Void> trigger;
			i.change.swap(trigger);
			Promise<Void> noDestroy = trigger;  // See explanation of noDestroy in setUnconditional()

			if (i.value == defaultValue)
				items.erase(key);

			trigger.send(Void());
		}
	}
	void clear( K const& k ) { set(k, V()); }
	V const& get( K const& k ) {
		auto it = items.find(k);
		if (it != items.end())
			return it->second.value;
		else
			return defaultValue;
	}
	int count( K const& k ) {
		auto it = items.find(k);
		if (it != items.end())
			return 1;
		return 0;
	}
	virtual Future<Void> onChange( K const& k ) {	// throws broken_promise if this is destroyed
		auto &item = items[k];
		if (item.value == defaultValue)
			return destroyOnCancel( this, k, item.change.getFuture() );
		return item.change.getFuture();
	}
	std::vector<K> getKeys() {
		std::vector<K> keys;
		for(auto i = items.begin(); i != items.end(); ++i)
			keys.push_back( i->first );
		return keys;
	}
	void resetNoWaiting() {
		for(auto i = items.begin(); i != items.end(); ++i)
			ASSERT( i->second.change.getFuture().getFutureReferenceCount() == 1 );
		items.clear();
	}

protected:
	// Invariant: Every item in the map either has value!=defaultValue xor a destroyOnCancel actor waiting on change.getFuture()
	struct P {
		V value;
		Promise<Void> change;
		P() : value() {}
	};
	std::map<K, P> items;
	const V defaultValue;
	bool destructing;

	void setUnconditional( K const& k, V const& v, P& i ) {
		Promise<Void> trigger;
		i.change.swap(trigger);
		Promise<Void> noDestroy = trigger;  // The send(Void()) or even V::operator= could cause destroyOnCancel,
			                                // which could undo the change to i.value here.  Keeping the promise reference count >= 2
			                                // prevents destroyOnCancel from erasing anything from the map.

		if (v == defaultValue)
			items.erase(k);
		else
			i.value = v;

		trigger.send(Void());
	}

	ACTOR Future<Void> destroyOnCancel( AsyncMap* self, K key, Future<Void> change ) {
		try {
			Void _ = wait(change);
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled && !self->destructing && change.getFutureReferenceCount()==1 && change.getPromiseReferenceCount()==1) {
				if(EXPENSIVE_VALIDATION) {
					auto& p = self->items[key];
					ASSERT(p.change.getFuture() == change);
				}
				self->items.erase(key);
			}
			throw;
		}
	}
};

template <class V>
class AsyncVar : NonCopyable, public ReferenceCounted<AsyncVar<V>> {
public:
	AsyncVar() : value() {}
	AsyncVar( V const& v ) : value(v) {}
	AsyncVar(AsyncVar&& av) : value(std::move(av.value)), nextChange(std::move(av.nextChange)) {}
	void operator=(AsyncVar&& av) { value = std::move(av.value); nextChange = std::move(av.nextChange); }

	V const& get() const {
		return value;
	}
	Future<Void> onChange() const {
		return nextChange.getFuture();
	}
	void set( V const& v ) {
		if (v != value)
			setUnconditional(v);
	}
	void setUnconditional( V const& v ) {
		Promise<Void> trigger;
		this->nextChange.swap(trigger);
		this->value = v;
		trigger.send(Void());
	}

private:
	V value;
	Promise<Void> nextChange;
};

class AsyncTrigger : NonCopyable {
public:
	AsyncTrigger() {}
	AsyncTrigger(AsyncTrigger&& at) : v(std::move(at.v)) {}
	void operator=(AsyncTrigger&& at) { v = std::move(at.v); }
	Future<Void> onTrigger() {
		return v.onChange();
	}
	void trigger() {
		v.setUnconditional(Void());
	}
private:
	AsyncVar<Void> v;
};

class Debouncer : NonCopyable {
public:
	explicit Debouncer( double delay ) { worker = debounceWorker(this, delay); }
	Debouncer(Debouncer&& at) : input(std::move(at.input)), output(std::move(at.output)) {}
	void operator=(Debouncer&& at) { input = std::move(at.input); output = std::move(at.output); }
	Future<Void> onTrigger() {
		return output.onChange();
	}
	void trigger() {
		input.setUnconditional(Void());
	}
private:
	AsyncVar<Void> input;
	AsyncVar<Void> output;
	Future<Void> worker;

	ACTOR Future<Void> debounceWorker( Debouncer* self, double bounceTime ) {
		loop {
			Void _ = wait( self->input.onChange() );
			loop {
				choose {
					when(Void _ = wait( self->input.onChange() )) {}
					when(Void _ = wait( delay(bounceTime) )) { break; }
				}
			}
			self->output.setUnconditional(Void());
		}
	}
};

ACTOR template <class T> Future<Void> asyncDeserialize( Reference<AsyncVar<Standalone<StringRef>>> input, Reference<AsyncVar<Optional<T>>> output ) {
	loop {
		if (input->get().size())
			output->set( BinaryReader::fromStringRef<T>( input->get(), IncludeVersion() ) );
		else
			output->set( Optional<T>() );
		Void _ = wait( input->onChange() );
	}
}

ACTOR template <class V, class T>
void forwardVector( Future<V> values, std::vector<Promise<T>> out ) {
	V in = wait( values );
	ASSERT (in.size() == out.size());
	for(int i=0; i<out.size(); i++)
		out[i].send( in[i] );
}

Future<bool> allTrue( const std::vector<Future<bool>>& all );
Future<Void> cancelOnly( std::vector<Future<Void>> const& futures );
Future<Void> timeoutWarningCollector( FutureStream<Void> const& input, double const& logDelay, const char* const& context, UID const& id );
Future<bool> quorumEqualsTrue( std::vector<Future<bool>> const& futures, int const& required );

ACTOR template <class T>
Future<Void> streamHelper( PromiseStream<T> output, PromiseStream<Error> errors, Future<T> input ) {
	try {
		T value = wait(input);
		output.send(value);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		errors.send(e);
	}
	return Void();
}

template <class T>
Future<Void> makeStream( const std::vector<Future<T>>& futures, PromiseStream<T>& stream, PromiseStream<Error>& errors ) {
	std::vector<Future<Void>> forwarders;
	for(int f=0; f<futures.size(); f++)
		forwarders.push_back( streamHelper( stream, errors, futures[f] ) );
	return cancelOnly(forwarders);
}

template <class T>
class QuorumCallback;

template <class T>
struct Quorum : SAV<Void> {
	int antiQuorum;
	int count;

	static inline int sizeFor(int count) {
		return sizeof(Quorum<T>) + sizeof(QuorumCallback<T>)*count;
	}

	virtual void destroy() {
		int size = sizeFor(this->count);
		this->~Quorum();
		freeFast(size, this);
	}
	virtual void cancel() {
		int cancelled_callbacks = 0;
		for (int i = 0; i < count; i++)
			if (callbacks()[i].next) {
				callbacks()[i].remove();
				callbacks()[i].next = 0;
				++cancelled_callbacks;
			}
		if (canBeSet())
			sendError(actor_cancelled());
		for (int i = 0; i < cancelled_callbacks; i++)
			delPromiseRef();
	}
	explicit Quorum(int quorum, int count) : SAV<Void>(1, count), antiQuorum(count - quorum + 1), count(count) {
		if (!quorum) this->send(Void());
	}
	void oneSuccess() {
		if (getPromiseReferenceCount() == antiQuorum && canBeSet())
			this->sendAndDelPromiseRef(Void());
		else
			delPromiseRef();
	}
	void oneError(Error err) {
		if (canBeSet())
			this->sendErrorAndDelPromiseRef(err);
		else
			delPromiseRef();
	}

	QuorumCallback<T>* callbacks() { return (QuorumCallback<T>*)(this + 1); }
};

template <class T>
class QuorumCallback : public Callback<T> {
public:
	QuorumCallback(Future<T> future, Quorum<T>* head)
		: head(head)
	{
		future.addCallbackAndClear(this);
	}
	virtual void fire(const T& value) {
		Callback<T>::remove();
		Callback<T>::next = 0;
		head->oneSuccess();
	}
	virtual void error(Error error) {
		Callback<T>::remove();
		Callback<T>::next = 0;
		head->oneError(error);
	}

private:
	Quorum<T>* head;
};

template <class T>
Future<Void> quorum(std::vector<Future<T>> const& results, int n) {
	ASSERT(n >= 0 && n <= results.size());

	int size = Quorum<T>::sizeFor(results.size());
	Quorum<T>* q = new (allocateFast(size)) Quorum<T>(n, results.size());

	QuorumCallback<T>* nextCallback = q->callbacks();
	for (auto & r : results) {
		if (r.isReady()) {
			nextCallback->next = 0;
			if (r.isError())
				q->oneError(r.getError());
			else
				q->oneSuccess();
		}
		else
			new (nextCallback) QuorumCallback<T>(r, q);
		++nextCallback;
	}
	return Future<Void>(q);
}

ACTOR template <class T>
Future<Void> smartQuorum( std::vector<Future<T>> results, int required, double extraSeconds, int taskID = TaskDefaultDelay ) {
	Void _ = wait(quorum(results, required));
	choose {
		when (Void _ = wait(quorum(results, (int)results.size()))) {return Void();}
		when (Void _ = wait(delay(extraSeconds, taskID))) {return Void(); }
	}
}

template <class T>
Future<Void> waitForAll( std::vector<Future<T>> const& results ) {
	if (results.empty()) return Void();
	return quorum( results, (int)results.size() );
}

template <class T>
Future<Void> waitForAny( std::vector<Future<T>> const& results ) {
	if (results.empty()) return Void();
	return quorum( results, 1 );
}

ACTOR static Future<bool> shortCircuitAny( std::vector<Future<bool>> f )
{
	std::vector<Future<Void>> sc;
	for(Future<bool> fut : f) {
		sc.push_back(returnIfTrue(fut));
	}

	choose {
		when( Void _ = wait( waitForAll( f ) ) ) {
			// Handle a possible race condition? If the _last_ term to
			// be evaluated triggers the waitForAll before bubbling
			// out of the returnIfTrue quorum
			for ( auto fut : f ) {
				if ( fut.get() ) {
					return true;
				}
			}
			return false;
		}
		when( Void _ = wait( waitForAny( sc ) ) ) {
			return true;
		}
	}
}

ACTOR template <class T>
Future<std::vector<T>> getAll( std::vector<Future<T>> input ) {
	if (input.empty()) return std::vector<T>();
	Void _ = wait( quorum( input, input.size() ) );

	std::vector<T> output;
	for(int i=0; i<input.size(); i++)
		output.push_back( input[i].get() );
	return output;
}

ACTOR template <class T>
Future<std::vector<T>> appendAll( std::vector<Future<std::vector<T>>> input ) {
	Void _ = wait( quorum( input, input.size() ) );

	std::vector<T> output;
	for(int i=0; i<input.size(); i++) {
		auto const& r = input[i].get();
		output.insert( output.end(), r.begin(), r.end() );
	}
	return output;
}

ACTOR template <class T> Future<Void> onEqual( Future<T> in, T equalTo ) {
	T t = wait(in);
	if ( t == equalTo )
		return Void();
	Void _ = wait(Never());  // never return
	throw internal_error();  // does not happen
}

ACTOR template <class T>
Future<Void> success( Future<T> of ) {
	T t = wait( of );
	return Void();
}

ACTOR template <class T>
Future<Void> ready( Future<T> f ) {
	try {
		T _ = wait( f );
	} catch (...) {
	}
	return Void();
}

ACTOR template <class T>
Future<T> waitAndForward( FutureStream<T> input ) {
	T output = waitNext( input );
	return output;
}

ACTOR template <class T>
Future<T> reportErrorsExcept( Future<T> in, const char* context, UID id, std::set<int> const* pExceptErrors ) {
	try {
		T t = wait( in );
		return t;
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && (!pExceptErrors || !pExceptErrors->count(e.code())))
			TraceEvent(SevError, context, id).error(e);
		throw;
	}
}

template <class T>
Future<T> reportErrors( Future<T> const& in, const char* context, UID id = UID() ) {
	return reportErrorsExcept(in, context, id, NULL);
}

ACTOR template <class T>
Future<T> require( Future<Optional<T>> in, int errorCode ) {
	Optional<T> o = wait(in);
	if (o.present()) {
		return o.get();
	}
	else {
		throw Error(errorCode);
	}
}

ACTOR template <class T>
Future<T> waitForFirst( std::vector<Future<T>> items ) {
	state PromiseStream<T> resultStream;
	state PromiseStream<Error> errorStream;

	state Future<Void> forCancellation = makeStream( items, resultStream, errorStream );

	state FutureStream<T> resultFutureStream = resultStream.getFuture();
	state FutureStream<Error> errorFutureStream = errorStream.getFuture();

	choose {
		when (T val = waitNext( resultFutureStream )) {
			forCancellation = Future<Void>();
			return val;
		}
		when (Error e = waitNext( errorFutureStream )) {
			forCancellation = Future<Void>();
			throw e;
		}
	}
}

ACTOR template <class T>
Future<T> tag( Future<Void> future, T what ) {
	Void _ = wait(future);
	return what;
}

ACTOR template <class T>
Future<Void> tag( Future<Void> future, T what, PromiseStream<T> stream ) {
	Void _ = wait( future );
	stream.send( what );
	return Void();
}

ACTOR template <class T>
Future<T> tagError( Future<Void> future, Error e) {
	Void _ = wait(future);
	throw e;
}

//If the future is ready, yields and returns. Otherwise, returns when future is set.
template <class T>
Future<T> orYield( Future<T> f ) {
	if(f.isReady()) {
		if(f.isError())
			return tagError<T>(yield(), f.getError());
		else
			return tag(yield(), f.get());
	}
	else
		return f;
}

static Future<Void> orYield( Future<Void> f ) {
	if(f.isReady()) {
		if(f.isError())
			return tagError<Void>(yield(), f.getError());
		else
			return yield();
	}
	else
		return f;
}

ACTOR template <class T> Future<T> chooseActor( Future<T> lhs, Future<T> rhs ) {
	choose {
		when ( T t = wait(lhs) ) { return t; }
		when ( T t = wait(rhs) ) { return t; }
	}
}

// set && set -> set
// error && x -> error
// all others -> unset
static Future<Void> operator &&( Future<Void> const& lhs, Future<Void> const& rhs ) {
	if(lhs.isReady()) {
		if(lhs.isError()) return lhs;
		else return rhs;
	}
	if(rhs.isReady()) {
		if(rhs.isError()) return rhs;
		else return lhs;
	}

	std::vector<Future<Void>> v;
	v.push_back( lhs );
	v.push_back( rhs );
	return waitForAll(v);
}

// error || unset -> error
// unset || unset -> unset
// all others -> set
inline Future<Void> operator ||( Future<Void> const& lhs, Future<Void> const& rhs ) {
	if(lhs.isReady()) {
		if(lhs.isError()) return lhs;
		if(rhs.isReady()) return rhs;
		return lhs;
	}

	return chooseActor( lhs, rhs );
}

ACTOR template <class T> Future<T> brokenPromiseToNever( Future<T> in ) {
	try {
		T t = wait(in);
		return t;
	} catch (Error& e) {
		if (e.code() != error_code_broken_promise)
			throw;
		Void _ = wait(Never());  // never return
		throw internal_error();  // does not happen
	}
}

ACTOR template <class T> Future<T> brokenPromiseToMaybeDelivered( Future<T> in ) {
	try {
		T t = wait(in);
		return t;
	} catch (Error& e) {
		if (e.code() == error_code_broken_promise) {
			throw request_maybe_delivered();
		}
		throw;
	}
}

ACTOR template <class T> void tagAndForward( Promise<T>* pOutputPromise, T value, Future<Void> signal ) {
	state Promise<T> out( std::move(*pOutputPromise) );
	Void _ = wait( signal );
	out.send(value);
}

ACTOR template <class T> void tagAndForwardError( Promise<T>* pOutputPromise, Error value, Future<Void> signal ) {
	state Promise<T> out( std::move(*pOutputPromise) );
	Void _ = wait( signal );
	out.sendError(value);
}

ACTOR template <class T> Future<T> waitOrError(Future<T> f, Future<Void> errorSignal) {
	choose {
		when(T val = wait(f)) {
			return val;
		}
		when(Void _ = wait(errorSignal)) {
			ASSERT(false);
			throw internal_error();
		}
	}
}

struct FlowLock : NonCopyable, public ReferenceCounted<FlowLock> {
	// FlowLock implements a nonblocking critical section: there can be only a limited number of clients executing code between
	// wait(take()) and release(). Not thread safe. take() returns only when the number of holders of the lock is fewer than the
	// number of permits, and release() makes the caller no longer a holder of the lock. release() only runs waiting take()rs
	// after the caller wait()s

	struct Releaser : NonCopyable {
		FlowLock* lock;
		int remaining;
		Releaser() : lock(0), remaining(0) {}
		Releaser( FlowLock& lock, int amount = 1 ) : lock(&lock), remaining(amount) {}
		Releaser(Releaser&& r) noexcept(true) : lock(r.lock), remaining(r.remaining) { r.remaining = 0; }
		void operator=(Releaser&& r) { if (remaining) lock->release(remaining); lock = r.lock; remaining = r.remaining; r.remaining = 0; }

		void release( int amount = -1 ) {
			if( amount == -1 || amount > remaining )
				amount = remaining;

			if (remaining)
				lock->release( amount );
			remaining -= amount;
		}

		~Releaser() { if (remaining) lock->release(remaining); }
	};

	FlowLock() : permits(1), active(0) {}
	explicit FlowLock(int permits) : permits(permits), active(0) {}

	Future<Void> take(int taskID = TaskDefaultYield, int amount = 1) {
		ASSERT(amount <= permits);
		if (active + amount <= permits) {
			active += amount;
			return safeYieldActor(this, taskID, amount);
		}
		return takeActor(this, taskID, amount);
	}
	void release( int amount = 1 ) {
		ASSERT( active > 0 || amount == 0 );
		active -= amount;

		while( !takers.empty() ) {
			if( active + takers.begin()->second <= permits ) {
				std::pair< Promise<Void>, int > next = std::move( *takers.begin() );
				active += next.second;
				takers.pop_front();
				next.first.send(Void());
			} else {
				break;
			}
		}
	}

	Future<Void> releaseWhen( Future<Void> const& signal, int amount = 1 ) { return releaseWhenActor( this, signal, amount ); }

	// returns when any permits are available, having taken as many as possible up to the given amount, and modifies amount to the number of permits taken
	Future<Void> takeUpTo(int& amount) {
		return takeMoreActor(this, &amount);
	}

	int available() const { return permits - active; }
	int activePermits() const { return active; }
	int waiters() const { return takers.size(); }
private:
	std::list< std::pair< Promise<Void>, int > > takers;
	const int permits;
	int active;
	Promise<Void> broken_on_destruct;

	ACTOR static Future<Void> takeActor(FlowLock* lock, int taskID, int amount) {
		state std::list<std::pair<Promise<Void>, int>>::iterator it = lock->takers.insert(lock->takers.end(), std::make_pair(Promise<Void>(), amount));

		try {
			Void _ = wait( it->first.getFuture() );
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				lock->takers.erase(it);
				lock->release(0);
			}
			throw;
		}
		try {
			double duration = BUGGIFY_WITH_PROB(.001) ? g_random->random01()*FLOW_KNOBS->BUGGIFY_FLOW_LOCK_RELEASE_DELAY : 0.0;
			choose{ when(Void _ = wait(delay(duration, taskID))) {}  // So release()ing the lock doesn't cause arbitrary code to run on the stack
					when(Void _ = wait(lock->broken_on_destruct.getFuture())) {} }
			return Void();
		} catch (...) {
			TEST(true); // If we get cancelled here, we are holding the lock but the caller doesn't know, so release it
			lock->release(amount);
			throw;
		}
	}

	ACTOR static Future<Void> takeMoreActor(FlowLock* lock, int* amount) {
		Void _ = wait(lock->take());
		int extra = std::min( lock->available(), *amount-1 );
		lock->active += extra;
		*amount = 1 + extra;
		return Void();
	}

	ACTOR static Future<Void> safeYieldActor(FlowLock* lock, int taskID, int amount) {
		try {
			choose{
				when(Void _ = wait(yield(taskID))) {}
				when(Void _ = wait(lock->broken_on_destruct.getFuture())) {}
			}
			return Void();
		} catch (Error& e) {
			lock->release(amount);
			throw;
		}
	}

	ACTOR static Future<Void> releaseWhenActor( FlowLock* self, Future<Void> signal, int amount ) {
		Void _ = wait(signal);
		self->release(amount);
		return Void();
	}
};

ACTOR template <class T>
Future<Void> yieldPromiseStream( FutureStream<T> input, PromiseStream<T> output, int taskID = TaskDefaultYield ) {
	loop {
		T f = waitNext( input );
		output.send( f );
		Void _ = wait( yield( taskID ) );
	}
}


struct YieldedFutureActor : SAV<Void>, ActorCallback<YieldedFutureActor, 1, Void>, FastAllocated<YieldedFutureActor> {
	Error in_error_state;

	typedef ActorCallback<YieldedFutureActor, 1, Void> CB1;

	using FastAllocated<YieldedFutureActor>::operator new;
	using FastAllocated<YieldedFutureActor>::operator delete;

	YieldedFutureActor(Future<Void> && f) : SAV<Void>(1, 1), in_error_state(Error::fromCode(UNSET_ERROR_CODE)) {
		f.addYieldedCallbackAndClear(static_cast< ActorCallback< YieldedFutureActor, 1, Void >* >(this));
	}

	void cancel()
	{
		if (!SAV<Void>::canBeSet()) return;  // Cancel could be invoked *by* a callback within finish().  Otherwise it's guaranteed that we are waiting either on the original future or on a delay().
		ActorCallback<YieldedFutureActor, 1, Void>::remove();
		SAV<Void>::sendErrorAndDelPromiseRef(actor_cancelled());
	}

	virtual void destroy() {
		delete this;
	}

	void a_callback_fire(ActorCallback<YieldedFutureActor, 1, Void>*, Void) {
		if (int16_t(in_error_state.code()) == UNSET_ERROR_CODE) {
			in_error_state = Error::fromCode(SET_ERROR_CODE);
			if (check_yield())
				doYield();
			else
				finish();
		} else {
			// We hit this case when and only when the delay() created by a previous doYield() fires.  Then we want to get at least one task done, regardless of what check_yield() would say.
			finish();
		}
	}
	void a_callback_error(ActorCallback<YieldedFutureActor, 1, Void>*, Error const& err) {
		ASSERT(int16_t(in_error_state.code()) == UNSET_ERROR_CODE);
		in_error_state = err;
		if (check_yield())
			doYield();
		else
			finish();
	}
	void finish() {
		ActorCallback<YieldedFutureActor, 1, Void>::remove();
		if (int16_t(in_error_state.code()) == SET_ERROR_CODE)
			SAV<Void>::sendAndDelPromiseRef(Void());
		else
			SAV<Void>::sendErrorAndDelPromiseRef(in_error_state);
	}
	void doYield() {
		// Since we are being fired, we are the first callback in the ring, and `prev` is the source future
		Callback<Void>* source = CB1::prev;
		ASSERT(source->next == static_cast<CB1*>(this));

		// Remove the source future from the ring.  All the remaining callbacks in the ring should be yielded, since yielded callbacks are installed at the end
		CB1::prev = source->prev;
		CB1::prev->next = static_cast<CB1*>(this);

		// The source future's ring is now empty, since we have removed all the callbacks
		source->next = source->prev = source;
		source->unwait();

		// Link all the callbacks, including this one, into the ring of a delay future so that after a short time they will be fired again
		delay(0, g_network->getCurrentTask()).addCallbackChainAndClear(static_cast< CB1* >(this));
	}
};

static Future<Void> yieldedFuture(Future<Void> f) {
	if (f.isReady())
		return yield();
	else
		return Future<Void>(new YieldedFutureActor(std::move(f)));
}

//An AsyncMap that uses a yieldedFuture in its onChange method.
template <class K, class V>
class YieldedAsyncMap : public AsyncMap<K, V> {
public:
	Future<Void> onChange(K const& k) {	// throws broken_promise if this is destroyed
		auto &item = AsyncMap<K, V>::items[k];
		if (item.value == AsyncMap<K, V>::defaultValue)
			return destroyOnCancelYield(this, k, item.change.getFuture());
		return yieldedFuture(item.change.getFuture());
	}

	ACTOR static Future<Void> destroyOnCancelYield( YieldedAsyncMap* self, K key, Future<Void> change ) {
		try {
			Void _ = wait(yieldedFuture(change));
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled && !self->destructing && change.getFutureReferenceCount() == 1 && change.getPromiseReferenceCount() == 1) {
				if(EXPENSIVE_VALIDATION) {
					auto& p = self->items[key];
					ASSERT(p.change.getFuture() == change);
				}
				self->items.erase(key);
			}
			throw;
		}
	}
};

ACTOR template <class T>
Future<T> delayActionJittered( Future<T> what, double time ) {
	Void _ = wait( delayJittered( time ) );
	T t = wait( what );
	return t;
}

class AndFuture {
public:
	AndFuture() { }

	AndFuture(AndFuture const& f) {
		futures = f.futures;
	}

	AndFuture(AndFuture&& f) noexcept(true) {
		futures = std::move(f.futures);
	}

	AndFuture(Future<Void> const& f) {
		futures.push_back(f);
	}

	AndFuture(Error const& e) {
		futures.push_back(e);
	}

	operator Future<Void>() {
		return getFuture();
	}

	void operator=(AndFuture const& f) {
		futures = f.futures;
	}

	void operator=(AndFuture&& f) noexcept(true) {
		futures = std::move(f.futures);
	}

	void operator=(Future<Void> const& f) {
		futures.push_back(f);
	}

	void operator=(Error const& e) {
		futures.push_back(e);
	}

	Future<Void> getFuture() {
		if(futures.empty())
			return Void();

		if(futures.size() == 1)
			return futures[0];

		Future<Void> f = waitForAll(futures);
		futures = std::vector<Future<Void>>();
		futures.push_back(f);
		return f;
	}

	bool isReady() {
		for( int i = futures.size() - 1; i >= 0; --i ) {
			if( !futures[i].isReady() ) {
				return false;
			}
			else if(!futures[i].isError()) {
				std::swap(futures[i], futures.back());
				futures.pop_back();
			}
		}
		return true;
	}

	bool isError() {
		for( int i = 0; i < futures.size(); i++ )
			if( futures[i].isError() )
				return true;
		return false;
	}

	void cleanup() {
		for( int i = 0; i < futures.size(); i++ ) {
			if( futures[i].isReady() && !futures[i].isError() ) {
				std::swap(futures[i--], futures.back());
				futures.pop_back();
			}
		}
	}

	void add(Future<Void> const& f) {
		if(!f.isReady() || f.isError())
			futures.push_back(f);
	}

	void add(AndFuture f) {
		add(f.getFuture());
	}

private:
	std::vector<Future<Void>> futures;
};

// Performs an unordered merge of a and b.
ACTOR template <class T>
Future<Void> unorderedMergeStreams(FutureStream<T> a, FutureStream<T> b, PromiseStream<T> output) {
	state Future<T> aFuture = waitAndForward(a);
	state Future<T> bFuture = waitAndForward(b);
	state bool aOpen = true;
	state bool bOpen = true;

	loop{
		try {
			choose {
				when(T val = wait(aFuture)) {
					output.send(val);
					aFuture = waitAndForward(a);
				}
				when(T val = wait(bFuture)) {
					output.send(val);
					bFuture = waitAndForward(b);
				}
			}
		}
		catch (Error &e) {
			if (e.code() != error_code_end_of_stream) {
				output.sendError(e);
				break;
			}

			ASSERT(!aFuture.isError() || !bFuture.isError() || aFuture.getError().code() == bFuture.getError().code());

			if (aFuture.isError()) {
				aFuture = Never();
				aOpen = false;
			}
			if (bFuture.isError()) {
				bFuture = Never();
				bOpen = false;
			}

			if (!aOpen && !bOpen) {
				output.sendError(e);
				break;
			}
		}
	}

	return Void();
}

// Returns the ordered merge of a and b, assuming that a and b are both already ordered (prefer a over b if keys are equal). T must be a class that implements compare()
ACTOR template <class T>
Future<Void> orderedMergeStreams( FutureStream<T> a, FutureStream<T> b, PromiseStream<T> output ) {
	state Optional<T> savedKVa;
	state bool aOpen;
	state Optional<T> savedKVb;
	state bool bOpen;

	aOpen = bOpen = true;

	loop {
		if ( aOpen && !savedKVa.present() ) {
			try {
				T KVa = waitNext( a );
				savedKVa = Optional<T>( KVa );
			} catch ( Error& e ) {
				if ( e.code() == error_code_end_of_stream ) {
					aOpen = false;
					if (!bOpen) {
						output.sendError(e);
					}
				} else {
					output.sendError(e);
					break;
				}
			}
		}
		if ( bOpen && !savedKVb.present() ) {
			try {
				T KVb = waitNext( b );
				savedKVb = Optional<T>( KVb );
			} catch ( Error& e ) {
				if ( e.code() == error_code_end_of_stream ) {
					bOpen = false;
					if (!aOpen) {
						output.sendError(e);
					}
				} else {
					output.sendError(e);
					break;
				}
			}
		}

		if (!aOpen) {
			output.send( savedKVb.get() );
			savedKVb = Optional<T>();
		} else if (!bOpen) {
			output.send( savedKVa.get() );
			savedKVa = Optional<T>();
		} else {
			int cmp = savedKVa.get().compare( savedKVb.get() );

			if ( cmp == 0 ) {
				// prefer a
				output.send( savedKVa.get() );
				savedKVa = Optional<T>();
				savedKVb = Optional<T>();
			} else if ( cmp < 0 ) {
				output.send( savedKVa.get() );
				savedKVa = Optional<T>();
			} else {
				output.send( savedKVb.get() );
				savedKVb = Optional<T>();
			}
		}
	}

	return Void();
}


ACTOR template<class T>
Future<Void> timeReply(Future<T> replyToTime, PromiseStream<double> timeOutput){
	state double startTime = now();
	try {
		T _ = wait(replyToTime);
		Void _ = wait( delay(0) );
		timeOutput.send(now() - startTime);
	} catch( Error &e ) {
		// Ignore broken promises.  They typically occur during shutdown and our callers don't want to have to create brokenPromiseToNever actors to ignore them.  For what it's worth we are breaking timeOutput to pass the pain along.
		if( e.code() != error_code_broken_promise )
			throw;
	}
	return Void();
}


#endif
