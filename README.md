Matlab BDB
==========

Persistent key-value storage for matlab.

Matlab BDB is yet another storage for Matlab. It is a key-value storage for
matlab value objects, and suitable for storing a lot of small to medium
sized data. The implementation is based on Berkeley DB.

Contents
--------

The package contains following files.

    +bdb/          API functions.
    +bdb/private/  Internal driver functions.
    test/          Optional functions to check the functionality.
    README.md      This file.

Compile
-------

The prerequisites are:

 * libdb
 * zlib

Have these libraries installed in the system. The `bdb.make` function builds
necessary dependent files. Check `bdb.make` for the detail of compile-time
options.

    >> bdb.make

API
---

Currently following functions are available from matlab. Check `help` for the
detail of each function.

    bdb.open     Open a Berkeley DB database.
    bdb.close    Close the database.
    bdb.put      Store a key-value pair.
    bdb.get      Retrieve a value given key.
    bdb.delete   Delete an entry for a key.
    bdb.keys     Return a list of keys in the database.
    bdb.values   Return a list of values in the database.
    bdb.stat     Get a statistics of the database.
    bdb.exists   Check if an entry exists.

Example
-------

Here is a quick usage example.

    >> bdb.open('test.db');    % Open a database.
    >> bdb.put('foo', 'bar');  % Store a key-value pair.
    >> bdb.put(2, magic(4));   % Store a key-value pair.
    >> a = bdb.get('foo');     % Retrieve a value.
    >> b = bdb.get(2);         % Retrieve a value.
    >> flag = bdb.exists(3);   % Check if a key exists.
    >> bdb.delete('a');        % Delete an entry.
    >> keys = bdb.keys();      % All keys at once.
    >> values = bdb.values();  % All values at once.
    >> bdb.close();            % Finish the session.

To open multiple sessions, use the session id returned from `bdb.open`.

    >> id = bdb.open('test.db');
    >> bdb.put(id, 'a', 'bar');
    >> a = bdb.get(id, 'a');
    >> bdb.close(id);

To use a database in multiple processes, open the database in an
environment. Note that you need to create an environment directory
if not existing. This will improve concurrency support.

    >> mkdir('/path/to/test_db_env');
    >> id = bdb.open('test.db', '/path/to/test_db_env');

Notes
-----

### Data compression

Data compression is enabled by default to save storage space. It is possible
to disable data compression at compile time with `--enable_zlib` option.

    >> bdb.make('--enable_zlib', false)

Compression leads to smaller storage size with the cost of slower speed. In
general, when data contain regular patterns, such as when data are all-zero,
compression makes the biggest effect. However, if data are close to random,
there is no advantage in the resulting storage size.

### Limited support

The implementation uses undocumented matlab mex functions `mxSerialize` and
`mxDeserialize`. The behavior of these functions are not guaranteed to work in
all versions of matlab, and may change in the future matlab release.

License
-------

The code may be redistributed under BSD 3-clause license.
