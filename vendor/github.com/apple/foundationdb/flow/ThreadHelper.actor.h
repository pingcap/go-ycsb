/*
 * ThreadHelper.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated
// version of this file.  In intellisense use the source version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_THREADHELPER_ACTOR_G_H)
		#define FLOW_THREADHELPER_ACTOR_G_H
		#include "ThreadHelper.actor.g.h"
#elif !defined(FLOW_THREADHELPER_ACTOR_H)
		#define FLOW_THREADHELPER_ACTOR_H

#include "flow/flow.h"

// template <class F>
// void onMainThreadVoid( F f ) {
// 	Promise<Void> signal;
// 	doOnMainThreadVoid( signal.getFuture(), f );
// 	g_network->onMainThread( std::move(signal), TaskDefaultOnMainThread );
// }

template <class F>
void onMainThreadVoid( F f, Error* err, int taskID = TaskDefaultOnMainThread ) {
	Promise<Void> signal;
	doOnMainThreadVoid( signal.getFuture(), f, err );
	g_network->onMainThread( std::move(signal), taskID );
}

struct ThreadCallback {
	virtual bool canFire(int notMadeActive) = 0;
	virtual void fire(const Void &unused, int& userParam) = 0;
	virtual void error(const Error&, int& userParam) = 0;
	virtual ThreadCallback* addCallback(ThreadCallback *cb);

	virtual bool contains(ThreadCallback *cb) {
		return false;
	}

	virtual void clearCallback(ThreadCallback *cb) {
		// If this is the only registered callback this will be called with (possibly) arbitrary pointers
	}

	virtual void destroy() {
		UNSTOPPABLE_ASSERT(false);
	}
	virtual bool isMultiCallback() const {
		return false;
	}
};

class ThreadMultiCallback : public ThreadCallback, public FastAllocated<ThreadMultiCallback> {
public:
	ThreadMultiCallback() { }

	virtual ThreadCallback* addCallback(ThreadCallback *callback) {
		UNSTOPPABLE_ASSERT(callbackMap.count(callback) == 0); //May be triggered by a waitForAll on a vector with the same future in it more than once
		callbackMap[callback] = callbacks.size();
		callbacks.push_back(callback);
		return (ThreadCallback*)this;
	}

	virtual bool contains(ThreadCallback *cb) {
		return callbackMap.count(cb) != 0;
	}

	virtual void clearCallback(ThreadCallback *callback) {
		auto it = callbackMap.find(callback);
		if (it == callbackMap.end())
			return;

		UNSTOPPABLE_ASSERT(it->second < callbacks.size() && it->second >= 0);

		if (it->second != callbacks.size() - 1) {
			callbacks[it->second] = callbacks.back();
			callbackMap[callbacks[it->second]] = it->second;
		}

		callbacks.pop_back();
		callbackMap.erase(it);
	}

	virtual bool canFire(int notMadeActive) {
		return true;
	}

	virtual void fire(const Void& value, int& loopDepth) {
		if (callbacks.size() > 10000)
			TraceEvent(SevWarn, "LargeMultiCallback").detail("CallbacksSize", callbacks.size());

		UNSTOPPABLE_ASSERT(loopDepth == 0);

		while (callbacks.size()) {
			auto cb = callbacks.back();
			callbacks.pop_back();
			callbackMap.erase(cb);
			if (cb->canFire(0)) {
				int ld = 0;
				cb->fire(value, ld);
			}
		}
	}

	virtual void error(const Error& err, int& loopDepth) {
		if (callbacks.size() > 10000)
			TraceEvent(SevWarn, "LargeMultiCallback").detail("CallbacksSize", callbacks.size());

		UNSTOPPABLE_ASSERT(loopDepth == 0);

		while (callbacks.size()) {
			auto cb = callbacks.back();
			callbacks.pop_back();
			callbackMap.erase(cb);
			if (cb->canFire(0)) {
				int ld = 0;
				cb->error(err, ld);
			}
		}
	}

	virtual void destroy() {
		UNSTOPPABLE_ASSERT(callbacks.empty());
		delete this;
	}

	virtual bool isMultiCallback() const {
		return true;
	}

private:
	std::vector<ThreadCallback*> callbacks;
	std::unordered_map<ThreadCallback*, int> callbackMap;
};

struct SetCallbackResult {
	enum Result { FIRED, CANNOT_FIRE, CALLBACK_SET };
};

class ThreadSingleAssignmentVarBase {
public:
	enum Status { Unset, NeverSet, Set, ErrorSet };  // order is important
	// volatile long referenceCount;
	ThreadSpinLock mutex;
	Status status;
	Error error;
	ThreadCallback *callback;

	bool isReady() { 
		ThreadSpinLockHolder holder(mutex);
		return isReadyUnsafe();
	}

	bool isError() { 
		ThreadSpinLockHolder holder(mutex);
		return isErrorUnsafe();
	}

	int getErrorCode() {
		ThreadSpinLockHolder holder(mutex);
		if (!isReadyUnsafe()) return error_code_future_not_set;
		if (!isErrorUnsafe()) return error_code_success;
		return error.code();
	}

	bool canBeSet() { 
		ThreadSpinLockHolder holder(mutex);
		return canBeSetUnsafe();
	}

	class BlockCallback : public ThreadCallback {
	public:
		Event ev;

		BlockCallback( ThreadSingleAssignmentVarBase& sav ) { int ignore=0; sav.callOrSetAsCallback(this,ignore,0); ev.block(); }

		virtual bool canFire(int notMadeActive) { return true; }
		virtual void fire(const Void &unused, int& userParam) { ev.set(); }
		virtual void error(const Error&, int& userParam) { ev.set(); }
	};

	void blockUntilReady() {
		if(isReadyUnsafe()) {
			ThreadSpinLockHolder holder(mutex);
			ASSERT(isReadyUnsafe());
		}
		else {
			BlockCallback cb( *this );
		}
	}

	ThreadSingleAssignmentVarBase() : status(Unset), callback(NULL), valueReferenceCount(0) {} //, referenceCount(1) {}
	~ThreadSingleAssignmentVarBase() { 
		this->mutex.assertNotEntered(); 

		if(callback)
			callback->destroy();
	}

	virtual void addref( ) = 0;
	virtual void delref( ) = 0;

	void send(Never) {
		if (TRACE_SAMPLE()) TraceEvent(SevSample, "Promise_sendNever");
		ThreadSpinLockHolder holder(mutex);
		if (!canBeSetUnsafe())
			ASSERT(false);  // Promise fulfilled twice
		this->status = NeverSet;
	}

	void sendError(const Error& err) {
		if (TRACE_SAMPLE()) TraceEvent(SevSample, "Promise_sendError").detail("ErrorCode", err.code());
		this->mutex.enter();
		if (!canBeSetUnsafe()) {
			this->mutex.leave();
			ASSERT(false);  // Promise fulfilled twice
		}
		error = err;
		status = ErrorSet; 
		if (!callback) {
			this->mutex.leave();
			return;
		}
		auto func = callback;
		if (!callback->isMultiCallback())
			callback = NULL;

		if (!func->canFire(0)) {
			this->mutex.leave();
		} else {
			this->mutex.leave();

			//Thread safe because status is now ErrorSet and callback is NULL, meaning than callback cannot change
			int userParam = 0;
			func->error(err, userParam);
		}
	}

	SetCallbackResult::Result callOrSetAsCallback( ThreadCallback* callback, int& userParam1, int notMadeActive ) {
		this->mutex.enter();
		if (isReadyUnsafe()) {
			if (callback->canFire(notMadeActive)) {
				this->mutex.leave();

				//Thread safe because the Future is ready, meaning that status and this->error will not change
				if (status == ErrorSet) {
					auto error = this->error;	// Since callback might free this
					callback->error( error, userParam1 );
				} else {
					callback->fire( Void(), userParam1 );
				}

				return SetCallbackResult::FIRED;
			} else {
				this->mutex.leave();
				return SetCallbackResult::CANNOT_FIRE;
			}
		} else {
			if (this->callback)
				this->callback = this->callback->addCallback( callback );
			else
				this->callback = callback;

			this->mutex.leave();
			return SetCallbackResult::CALLBACK_SET;
		}
	}

	// If this function returns false, then this SAV has already been set and the callback has been or will be called.
	// If this function returns true, then the callback has not and will not be called by this SAV (unless it is set later).
	// This doesn't clear callbacks that are nested multiple levels inside of multi-callbacks
	bool clearCallback( ThreadCallback* cb ) {
		this->mutex.enter();

		//If another thread is calling fire in send/sendError, it would be unsafe to clear the callback
		if (isReadyUnsafe()) {
			this->mutex.leave();
			return false;
		}

		// Only clear the callback if it belongs to the caller, because
		// another actor could be waiting on it now!
		if (callback == cb)
			callback = NULL;
		else if (callback != NULL)
			callback->clearCallback( cb );

		this->mutex.leave();

		return true;
	}

	void setCancel( Future<Void> && cf ) {
		cancelFuture = std::move(cf);
	}

	virtual void cancel() {
		// Cancels the action and decrements the reference count by 1
		// The if statement is just an optimization. It's ok if we take the wrong path due to a race
		if(isReadyUnsafe())
			delref();
		else
			onMainThreadVoid( [this](){ this->cancelFuture.cancel(); this->delref(); }, NULL );
	}

	void releaseMemory() {
		ThreadSpinLockHolder holder(mutex);
		if (--valueReferenceCount == 0)
			cleanupUnsafe();
	}

private:
	Future<Void> cancelFuture;
	int32_t valueReferenceCount;

protected:
	bool isReadyUnsafe() const { return status >= Set; }
	bool isErrorUnsafe() const { return status == ErrorSet; }
	bool canBeSetUnsafe() const { return status == Unset; }

	void addValueReferenceUnsafe() {
		++valueReferenceCount;
	}

	virtual void cleanupUnsafe() {
		if(status != ErrorSet) {
			error = future_released();
			status = ErrorSet;
		}

		valueReferenceCount = 0;
		this->addref();
		cancel();
	}
};

template <class T>
class ThreadSingleAssignmentVar : public ThreadSingleAssignmentVarBase, /* public FastAllocated<ThreadSingleAssignmentVar<T>>,*/ public ThreadSafeReferenceCounted<ThreadSingleAssignmentVar<T>>
{
public:
	virtual ~ThreadSingleAssignmentVar() {}

	T value;

	T get() {
		ThreadSpinLockHolder holder(mutex);
		if( !isReadyUnsafe() )
			throw future_not_set();
		if ( isErrorUnsafe() )
			throw error;

		addValueReferenceUnsafe();
		return value;
	}

	virtual void addref( ) {
		ThreadSafeReferenceCounted<ThreadSingleAssignmentVar<T>>::addref( );
	}

	virtual void delref( ) {
		ThreadSafeReferenceCounted<ThreadSingleAssignmentVar<T>>::delref( );
	}

	void send(const T& value) {
		if (TRACE_SAMPLE()) TraceEvent(SevSample, "Promise_send");
		this->mutex.enter();
		if (!canBeSetUnsafe()) {
			this->mutex.leave();
			ASSERT(false);  // Promise fulfilled twice
		}
		this->value = value;		//< Danger: polymorphic operation inside lock
		this->status = Set;
		if (!callback) {
			this->mutex.leave();
			return;
		}

		auto func = callback;
		if(!callback->isMultiCallback())
			callback = NULL;

		if (!func->canFire(0)) {
			this->mutex.leave();
		} else {
			this->mutex.leave();

			//Thread safe because status is now Set and callback is NULL, meaning than callback cannot change
			int userParam = 0;
			func->fire(Void(), userParam);
		}
	}

	virtual void cleanupUnsafe() {
		value = T();
		ThreadSingleAssignmentVarBase::cleanupUnsafe();
	}
};


template <class T>
class ThreadFuture
{
public:
	T get() { return sav->get(); }
	T getBlocking() {
		sav->blockUntilReady();
		return sav->get();
	}

	void blockUntilReady() {
		sav->blockUntilReady();
	}

	bool isValid() const {
		return sav != 0;
	}
	bool isReady() {
		return sav->isReady();
	}
	bool isError() {
		return sav->isError();
	}
	Error& getError() {
		if( !isError() )
			throw future_not_error();

		return sav->error;
	}

	SetCallbackResult::Result callOrSetAsCallback( ThreadCallback* callback, int& userParam1, int notMadeActive ) {
		return sav->callOrSetAsCallback(callback, userParam1, notMadeActive);
	}
	bool clearCallback(ThreadCallback* cb) {
		return sav->clearCallback(cb);
	}

	void cancel() {
		extractPtr()->cancel();
	}

	ThreadFuture() : sav(0) {}
	explicit ThreadFuture( ThreadSingleAssignmentVar<T> * sav ) : sav(sav) {
		// sav->addref();
	}
	ThreadFuture( const ThreadFuture<T>& rhs ) : sav(rhs.sav) {
		if (sav) sav->addref();
	}
	ThreadFuture(ThreadFuture<T>&& rhs) noexcept(true) : sav(rhs.sav) {
		rhs.sav = 0;
	}
	ThreadFuture( const T& presentValue ) 
		: sav(new ThreadSingleAssignmentVar<T>())
	{
		sav->send(presentValue);
	}
	ThreadFuture( Never )
		: sav(new ThreadSingleAssignmentVar<T>())
	{
	}
	ThreadFuture( const Error& error )
		: sav(new ThreadSingleAssignmentVar<T>())
	{
		sav->sendError(error);
	}
	~ThreadFuture() {
		if (sav) sav->delref();
	}
	void operator=(const ThreadFuture<T>& rhs) {
		if (rhs.sav) rhs.sav->addref();
		if (sav) sav->delref();
		sav = rhs.sav;
	}
	void operator=(ThreadFuture<T>&& rhs) noexcept(true) {
		if (sav != rhs.sav) {
			if (sav) sav->delref();
			sav = rhs.sav;
			rhs.sav = 0;
		}
	}
	bool operator == (const ThreadFuture& rhs) { return rhs.sav == sav; }
	bool operator != (const ThreadFuture& rhs) { return rhs.sav != sav; }

	ThreadSingleAssignmentVarBase* getPtr() const { return sav; }
	ThreadSingleAssignmentVarBase* extractPtr() { auto *p = sav; sav = NULL; return p; }

private:
	ThreadSingleAssignmentVar<T>* sav;
};

//A callback class used to convert a ThreadFuture into a Future
template<class T>
struct CompletionCallback : public ThreadCallback, ReferenceCounted<CompletionCallback<T>> {
	//The thread future being waited on
	ThreadFuture<T> threadFuture;

	//The promise whose future we are triggering when this callback gets called
	Promise<T> promise;

	//Unused
	int userParam;

	//Holds own reference to prevent deletion until callback is fired
	Reference<CompletionCallback<T>> self;

	CompletionCallback(ThreadFuture<T> threadFuture) {
		this->threadFuture = threadFuture;
	}

	bool canFire(int notMadeActive) {
		return true;
	}

	//Trigger the promise
	void fire(const Void& unused, int& userParam) {
		promise.send(threadFuture.get());
		self.clear();
	}

	//Send the error through the promise
	void error(const Error& e, int& userParam) {
		promise.sendError(e);
		self.clear();
	}
};

//Converts a ThreadFuture into a Future
//WARNING: This is not actually thread safe!  It can only be safely used from the main thread, on futures which are being set on the main thread
//FIXME: does not support cancellation
template<class T>
Future<T> unsafeThreadFutureToFuture(ThreadFuture<T> threadFuture) {
	Reference<CompletionCallback<T>> callback = Reference<CompletionCallback<T>>(new CompletionCallback<T>(threadFuture));
	callback->self = callback;
	threadFuture.callOrSetAsCallback(callback.getPtr(), callback->userParam, 0);
	return callback->promise.getFuture();
}

ACTOR template <class R, class F> Future<Void> doOnMainThread( Future<Void> signal, F f, ThreadSingleAssignmentVar<R> *result ) {
	try {
		Void _ = wait( signal );
		R r = wait( f() );
		result->send(r);
	} catch (Error& e) {
		if(!result->canBeSet()) {
			TraceEvent(SevError, "onMainThreadSetTwice").error(e,true);
		}
		result->sendError(e);
	}

	ThreadFuture<R> destroyResultAfterReturning(result);  // Call result->delref(), but only after our return promise is no longer referenced on this thread
	return Void();
}

ACTOR template <class F> void doOnMainThreadVoid( Future<Void> signal, F f, Error *err ) {
	Void _ = wait( signal );
	if (err && err->code() != invalid_error_code)
		return;
	try {
		f();
	} catch (Error& e) {
		if (err)
			*err = e;
	}
}

template <class F> ThreadFuture< decltype(fake<F>()().getValue()) > onMainThread( F f ) {
	Promise<Void> signal;
	auto returnValue = new ThreadSingleAssignmentVar< decltype(fake<F>()().getValue()) >();
	returnValue->addref(); // For the ThreadFuture we return
	Future<Void> cancelFuture = doOnMainThread<decltype(fake<F>()().getValue()), F>( signal.getFuture(), f, returnValue );
	returnValue->setCancel( std::move(cancelFuture) );
	g_network->onMainThread( std::move(signal), TaskDefaultOnMainThread );
	return ThreadFuture<decltype(fake<F>()().getValue())>( returnValue );
}

template <class V>
class ThreadSafeAsyncVar : NonCopyable, public ThreadSafeReferenceCounted<ThreadSafeAsyncVar<V>> {
public:
	struct State {
		State(V value, ThreadFuture<Void> onChange) : value(value), onChange(onChange) {}

		V value;
		ThreadFuture<Void> onChange;
	};

	ThreadSafeAsyncVar() : value(), nextChange(new ThreadSingleAssignmentVar<Void>()) {}
	ThreadSafeAsyncVar(V const& v) : value(v), nextChange(new ThreadSingleAssignmentVar<Void>()) {}

	State get() {
		ThreadSpinLockHolder holder(lock);
		nextChange->addref();
		return State(value, ThreadFuture<Void>(nextChange.getPtr()));
	}

	void set(V const& v, bool triggerIfSame = false) {
		Reference<ThreadSingleAssignmentVar<Void>> trigger(new ThreadSingleAssignmentVar<Void>());

		lock.enter();
		bool changed = this->value != v;
		if(changed || triggerIfSame) {
			std::swap(this->nextChange, trigger);
			this->value = v;
		}
		lock.leave();

		if(changed || triggerIfSame) {
			trigger->send(Void());
		}
	}

private:
	V value;
	Reference<ThreadSingleAssignmentVar<Void>> nextChange;
	ThreadSpinLock lock;
};

#endif
