/*
 * Locality.h
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

#ifndef FLOW_LOCALITY_H
#define FLOW_LOCALITY_H
#pragma once

#include "flow/flow.h"

struct ProcessClass {
	// This enum is stored in restartInfo.ini for upgrade tests, so be very careful about changing the existing items!
	enum ClassType { UnsetClass, StorageClass, TransactionClass, ResolutionClass, TesterClass, ProxyClass, MasterClass, StatelessClass, LogClass, ClusterControllerClass, InvalidClass = -1 };
	enum Fitness { BestFit, GoodFit, BestOtherFit, UnsetFit, WorstFit, ExcludeFit, NeverAssign };
	enum ClusterRole { Storage, TLog, Proxy, Master, Resolver, ClusterController };
	enum ClassSource { CommandLineSource, AutoSource, DBSource, InvalidSource = -1 };
	int16_t _class;
	int16_t _source;

public:
	ProcessClass() : _class( UnsetClass ), _source( CommandLineSource ) {}
	ProcessClass( ClassType type, ClassSource source ) : _class( type ), _source( source ) {}
	explicit ProcessClass( std::string s, ClassSource source ) : _source( source ) {
		if (s=="storage") _class = StorageClass;
		else if (s=="transaction") _class = TransactionClass;
		else if (s=="resolution") _class = ResolutionClass;
		else if (s=="proxy") _class = ProxyClass;
		else if (s=="master") _class = MasterClass;
		else if (s=="test") _class = TesterClass;
		else if (s=="unset") _class = UnsetClass;
		else if (s=="stateless") _class = StatelessClass;
		else if (s=="log") _class = LogClass;
		else if (s=="cluster_controller") _class = ClusterControllerClass;
		else _class = InvalidClass;
	}

	ProcessClass( std::string classStr, std::string sourceStr ) {
		if (classStr=="storage") _class = StorageClass;
		else if (classStr=="transaction") _class = TransactionClass;
		else if (classStr=="resolution") _class = ResolutionClass;
		else if (classStr=="proxy") _class = ProxyClass;
		else if (classStr=="master") _class = MasterClass;
		else if (classStr=="test") _class = TesterClass;
		else if (classStr=="unset") _class = UnsetClass;
		else if (classStr=="stateless") _class = StatelessClass;
		else if (classStr=="log") _class = LogClass;
		else if (classStr=="cluster_controller") _class = ClusterControllerClass;
		else _class = InvalidClass;

		if (sourceStr=="command_line") _source = CommandLineSource;
		else if (sourceStr=="configure_auto") _source = AutoSource;
		else if (sourceStr=="set_class") _source = DBSource;
		else _source = InvalidSource;
	}

	ClassType classType() const { return (ClassType)_class; }
	ClassSource classSource() const { return (ClassSource)_source; }

	bool operator == ( const ClassType& rhs ) const { return _class == rhs; }
	bool operator != ( const ClassType& rhs ) const { return _class != rhs; }

	bool operator == ( const ProcessClass& rhs ) const { return _class == rhs._class && _source == rhs._source; }
	bool operator != ( const ProcessClass& rhs ) const { return _class != rhs._class || _source != rhs._source; }

	std::string toString() const {
		switch (_class) {
			case UnsetClass: return "unset";
			case StorageClass: return "storage";
			case TransactionClass: return "transaction";
			case ResolutionClass: return "resolution";
			case ProxyClass: return "proxy";
			case MasterClass: return "master";
			case TesterClass: return "test";
			case StatelessClass: return "stateless";
			case LogClass: return "log";
			case ClusterControllerClass: return "cluster_controller";
			default: return "invalid";
		}
	}

	std::string sourceString() const {
		switch (_source) {
			case CommandLineSource: return "command_line";
			case AutoSource: return "configure_auto";
			case DBSource: return "set_class";
			default: return "invalid";
		}
	}

	Fitness machineClassFitness( ClusterRole role ) const ;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & _class & _source;
	}
};

struct LocalityData {
	std::map<Standalone<StringRef>, Optional<Standalone<StringRef>>>	_data;

	static const StringRef	keyProcessId;
	static const StringRef	keyZoneId;
	static const StringRef	keyDcId;
	static const StringRef	keyMachineId;
	static const StringRef	keyDataHallId;

public:
	LocalityData() {}

	LocalityData(Optional<Standalone<StringRef>> processID, Optional<Standalone<StringRef>> zoneID, Optional<Standalone<StringRef>> MachineID, Optional<Standalone<StringRef>> dcID ) {
		_data[keyProcessId] = processID;
		_data[keyZoneId] = zoneID;
		_data[keyMachineId] = MachineID;
		_data[keyDcId] = dcID;
	}

	bool operator == (LocalityData const& rhs) const {
		return ((_data.size() == rhs._data.size())													&&
					  (std::equal(_data.begin(), _data.end(), rhs._data.begin())));
	}

	Optional<Standalone<StringRef>>	get(StringRef key) const {
		auto pos = _data.find(key);
		return (pos == _data.end()) ? Optional<Standalone<StringRef>>() : pos->second;
	}

	void set(StringRef key, Optional<Standalone<StringRef>> value) {
		_data[key] = value;
	}

	bool	isPresent(StringRef key) const { return (_data.find(key) != _data.end()); }
	bool	isPresent(StringRef key, Optional<Standalone<StringRef>> value) const {
		auto pos = _data.find(key);
		return (pos != _data.end()) ? false : (pos->second == value);
	}

	std::string describeValue(StringRef key) const {
		auto value = get(key);
		return (value.present()) ? value.get().toString() : "[unset]";
	}

	std::string describeZone() const { return describeValue(keyZoneId); }
	std::string describeDataHall() const { return describeValue(keyDataHallId); }

	Optional<Standalone<StringRef>> processId() const { return get(keyProcessId); }
	Optional<Standalone<StringRef>> zoneId() const { return get(keyZoneId); }
	Optional<Standalone<StringRef>> machineId() const { return get(keyMachineId); }
	Optional<Standalone<StringRef>> dcId() const { return get(keyDcId); }
	Optional<Standalone<StringRef>> dataHallId() const { return get(keyDataHallId); }

	std::string toString() const {
		std::string	infoString;
		for (auto it = _data.rbegin(); !(it == _data.rend()); ++it) {
			if (infoString.length()) { infoString += " "; }
			infoString += it->first.printable() + "=";
			infoString += (it->second.present()) ? it->second.get().printable() : "[unset]";
		}
		return infoString;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		// Locality is persisted in the database inside StorageServerInterface, so changes here have to be
		// versioned carefully!
		if (ar.protocolVersion() >= 0x0FDB00A446020001LL) {
			Standalone<StringRef> key;
			Optional<Standalone<StringRef>> value;
			uint64_t mapSize = (uint64_t)_data.size();
			ar & mapSize;
			if (ar.isDeserializing) {
				for (size_t i = 0; i < mapSize; i++) {
					ar & key & value;
					_data[key] = value;
				}
			}
			else {
				for (auto it = _data.begin(); it != _data.end(); it++) {
					key = it->first;
					value = it->second;
					ar & key & value;
				}
			}
		}
		else {
			ASSERT(ar.isDeserializing);
			UID	zoneId, dcId, processId;
			ar & zoneId & dcId;
			set(keyZoneId, Standalone<StringRef>(zoneId.toString()));
			set(keyDcId, Standalone<StringRef>(dcId.toString()));

			if (ar.protocolVersion() >= 0x0FDB00A340000001LL) {
				ar & processId;
				set(keyProcessId, Standalone<StringRef>(processId.toString()));
			}
			else {
				int _machineClass = ProcessClass::UnsetClass;
				ar & _machineClass;
			}
		}
	}

	static const UID UNSET_ID;
};

static std::string describe(
		std::vector<LocalityData> const& items,
		StringRef const key,
		int max_items = -1 )
{
	if(!items.size())
		return "[no items]";
	std::string s;
	int count = 0;
	for(auto const& item : items) {
		if( ++count > max_items && max_items >= 0)
			break;
		if (count > 1) s += ",";
		s += item.describeValue(key);
	}
	return s;
}
static 	std::string describeZones( std::vector<LocalityData> const& items, int max_items = -1 ) {
	return describe(items, LocalityData::keyZoneId, max_items);
}
static 	std::string describeDataHalls( std::vector<LocalityData> const& items, int max_items = -1 ) {
	return describe(items, LocalityData::keyDataHallId, max_items);
}

struct ProcessData {
	LocalityData locality;
	ProcessClass processClass;
	NetworkAddress address;

	ProcessData() {}
	ProcessData( LocalityData locality, ProcessClass processClass, NetworkAddress address ) : locality(locality), processClass(processClass), address(address) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & locality & processClass & address;
	}

	struct sort_by_address {
		bool operator ()(ProcessData const&a, ProcessData const& b) const { return a.address < b.address; }
	};
};

template <class Interface, class Enable = void>
struct LBLocalityData {
	enum { Present = 0 };
	static LocalityData getLocality( Interface const& ) { return LocalityData(); }
	static NetworkAddress getAddress( Interface const& ) { return NetworkAddress(); }
};

// Template specialization that only works for interfaces with a .locality member.
//   If an interface has a .locality it must also have a .address()
template <class Interface>
struct LBLocalityData<Interface, typename std::enable_if< Interface::LocationAwareLoadBalance >::type> {
	enum { Present = 1 };
	static LocalityData getLocality( Interface const& i ) { return i.locality; }
	static NetworkAddress getAddress( Interface const& i ) { return i.address(); }
};

struct LBDistance {
	enum Type {
		SAME_MACHINE = 0,
		SAME_DC = 1,
		DISTANT = 2
	};
};

LBDistance::Type loadBalanceDistance( LocalityData const& localLoc, LocalityData const& otherLoc, NetworkAddress const& otherAddr );

#endif
