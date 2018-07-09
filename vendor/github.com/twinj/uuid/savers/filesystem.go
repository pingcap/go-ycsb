// Package savers provides implementations for the uuid.Saver interface.
package savers

import (
	"encoding/gob"
	"github.com/myesui/uuid"
	"log"
	"os"
	"path"
	"time"
)

var _ uuid.Saver = &FileSystemSaver{}

func init() {
	gob.Register(&uuid.Store{})
}

// FileSystemSaver implements the uuid.Saver interface.
type FileSystemSaver struct {
	// A file to save the state to
	// Used gob format on uuid.State entity
	file *os.File

	// Preferred location for the store
	Path string

	// Whether to log each save
	Report bool

	// The amount of time between each save call
	time.Duration

	// The next time to save
	uuid.Timestamp

	*log.Logger
}

func (o *FileSystemSaver) Init() uuid.Saver {
	o.Logger = log.New(os.Stderr, "uuid-saver", log.LstdFlags)
	o.Duration = 10 * time.Second
	o.Path = path.Join(os.TempDir(), "myesui-uuid-generator-fs-saver.gob")
	o.Timestamp = uuid.Now()
	return o
}

// Save saves the given store to the filesystem.
func (o *FileSystemSaver) Save(store uuid.Store) {

	if store.Timestamp >= o.Timestamp {
		err := o.openAndDo(o.encode, &store)
		if err == nil {
			if o.Report {
				o.Println("file system saver saved", store)
			}
		}
		o.Timestamp = store.Add(o.Duration)
	}
}

// Read reads and loads the Store from the filesystem.
func (o *FileSystemSaver) Read() (store uuid.Store, err error) {
	store = uuid.Store{}
	_, err = os.Stat(o.Path);
	if err != nil {
		if os.IsNotExist(err) {
			dir, file := path.Split(o.Path)
			if dir == "" || dir == "/" {
				dir = os.TempDir()
			}
			o.Path = path.Join(dir, file)

			err = os.MkdirAll(dir, os.ModeDir|0700)
			if err == nil {
				// If new encode blank store
				goto open
			}
		}
		goto failed
	}

	open:
	err = o.openAndDo(o.decode, &store)
	if err == nil {
		o.Println("file system saver created", o.Path)
		return
	}

	failed:
	o.Println("file system saver read error - will autogenerate", err)
	return
}

func (o *FileSystemSaver) openAndDo(fDo func(*uuid.Store), store *uuid.Store) (err error) {
	o.file, err = os.OpenFile(o.Path, os.O_RDWR|os.O_CREATE, os.ModeExclusive|0600)
	defer o.file.Close()
	if err == nil {
		fDo(store)
		return
	}
	o.Println("error opening file", err)
	return
}

func (o *FileSystemSaver) encode(pStore *uuid.Store) {
	// ensure reader state is ready for use
	enc := gob.NewEncoder(o.file)
	// swallow error for encode as its only for cyclic pointers
	enc.Encode(&pStore)
}

func (o *FileSystemSaver) decode(store *uuid.Store) {
	// ensure reader state is ready for use
	o.file.Seek(0, 0)
	dec := gob.NewDecoder(o.file)
	// swallow error for encode as its only for cyclic pointers
	dec.Decode(&store)
}
