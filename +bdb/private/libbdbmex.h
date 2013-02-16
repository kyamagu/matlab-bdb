// Berkeley DB matlab driver library.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#ifndef __LIBBDBMEX_H__
#define __LIBBDBMEX_H__

#include <db.h>
#include <map>
#include <mex.h>
#include <string>
#include <vector>

using namespace std;

// Alias for the mex error function.
#define ERROR(...) mexErrMsgIdAndTxt("bdb:error", __VA_ARGS__)

namespace bdbmex {

// Database record consisting of (key, value) pair of DBT struct.
class Record {
public:
  // Construct a new record for cursor operation.
  Record();
  // Construct a new record for retrieval.
  Record(const mxArray* key);
  // Construct a new record for store.
  Record(const mxArray* key, const mxArray* value);
  ~Record();
  // Get key.
  void get_key(mxArray** key);
  // Get value.
  void get_value(mxArray** value);
  // Mutable key.
  DBT* key() { return &key_; }
  // Mutable value.
  DBT* value() { return &value_; }

private:
  // Reset the record.
  void reset(u_int32_t key_flags, u_int32_t value_flags);
  // Set key.
  void set_key(const mxArray* key);
  // Set value.
  void set_value(const mxArray* value);
  // Serialize an mxArray.
  void serialize_mxarray(const mxArray* value, vector<uint8_t>* binary);
  // Deserialize an mxArray.
  void deserialize_mxarray(const vector<uint8_t>& binary, mxArray** value);
  // Serialize and compress an mxArray.
  void compress_mxarray(const mxArray* value, vector<uint8_t>* binary);
  // Decompress and deserialize mxArray.
  void decompress_mxarray(const vector<uint8_t>& binary, mxArray** value);

  // Key or the record.
  DBT key_;
  // Value of the record.
  DBT value_;
  // Temporary buffer for reference.
  vector<uint8_t> key_buffer_;
  // Temporary buffer for reference.
  vector<uint8_t> value_buffer_;
};

// Database cursor.
class Cursor {
public:
  Cursor() : cursor_(NULL) {}
  ~Cursor() {
    if (cursor_)
      cursor_->close(cursor_);
  }
  // Open a new cursor.
  int open(DB* database_) {
    return database_->cursor(database_, NULL, &cursor_, 0);
  }
  // Go to the next record.
  int next(Record* record) {
    return cursor_->get(cursor_, record->key(), record->value(), DB_NEXT);
  }
  // Go to the previous record.
  int prev(Record* record) {
    return cursor_->get(cursor_, record->key(), record->value(), DB_PREV);
  }
private:
  // Cursor pointer.
  DBC* cursor_;
};

// Database connection.
class Database {
public:
  Database();
  ~Database();
  // Open a connection. Optionally, it takes a path to the environment dir.
  bool open(const string& filename, const string& home_dir = "");
  // Return the last error code.
  int error_code();
  // Return the last error message.
  const char* error_message();
  // Return if the status is okay.
  bool ok() { return code_ == 0; }
  // Get an entry.
  bool get(const mxArray* key, mxArray** value);
  // Put an entry.
  bool put(const mxArray* key, const mxArray* value);
  // Delete an entry.
  bool del(const mxArray* key);
  // Check if the entry exists.
  bool exists(const mxArray* key, mxArray** value);
  // Return database statistics.
  bool stat(mxArray** output);
  // Dump keys in the database.
  bool keys(mxArray** output);
  // Dump values in the database.
  bool values(mxArray** output);

private:
  // Close the connection.
  bool close();

  // Last return code.
  int code_;
  // DB C object.
  DB* database_;
  // Environment C object.
  DB_ENV* environment_;
};

// Database session manager. Keep the state space.
class Sessions {
public:
  // Create a new connection.
  static int open(const string& filename, const string& home_dir = "");
  // Close the connection.
  static void close(int id);
  // Default id.
  static int default_id();
  // Last id.
  static int last_id();
  // Get the connection.
  static Database* get(int id);

private:
  // Session instantiation is prohibited.
  Sessions();
  // Destructor is prohibited.
  ~Sessions();

  // Connection pool.
  static map<int, Database> connections_;
  // Last id used.
  static int last_id_;
};

} // namespace bdbmex

#endif // __LIBBDBMEX_H__