// Berkeley DB matlab driver library.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#ifndef __BDBMEX_H__
#define __BDBMEX_H__

#include <cstring>
#include <map>
#include <memory>
#include <mex.h>
#include <db.h>
#include <string>
#include <vector>

using namespace std;

// Alias for the mex error function.
#define ERROR(...) mexErrMsgIdAndTxt("bdb:error", __VA_ARGS__)

// Hidden MEX API.
EXTERN_C mxArray* mxSerialize(const mxArray*);
EXTERN_C mxArray* mxDeserialize(const void*, size_t);

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

// Database session sessions. Container for Database objects.
class Sessions {
public:
  Sessions();
  ~Sessions();
  // Create a new connection.
  int open(const string& filename, const string& home_dir = "");
  // Close the connection.
  void close(int id);
  // Default id.
  int default_id() const;
  // Last id.
  int last_id() const;
  // Get the connection.
  Database* get(int id);

private:
  // Connection pool.
  map<int, Database> connections_;
  // Last id used.
  int last_id_;
};

// Abstract operation class. Child class must implement run() method.
//
//    auto_ptr<Operation> operation(OperationFactory::parse(nrhs, prhs));
//    operation->run(nlhs, plhs, nrhs - 1, prhs + 1);
//
class Operation {
public:
  // Destructor.
  virtual ~Operation() {}
  // Execute the operation.
  virtual void operator()(int nlhs,
                          mxArray *plhs[],
                          int nrhs,
                          const mxArray *prhs[]) = 0;

protected:
  // Default constructor is prohibited. Use parse() method.
  Operation() {}
  // Accessor for the sessions.
  static Sessions* sessions() { return &sessions_; }

private:
  // Database sessions.
  static Sessions sessions_;
};

// Base class for operation creators.
class OperationCreator {
public:
  // Register an operation in the constructor.
  OperationCreator(const string& name);
  // Implementation must return a new instance of the operation.
  virtual Operation* create() = 0;
};

// Implementation of the operation creator to be used as composition in an
// Operator class.
template <class OperationClass>
class OperationCreatorImpl : public OperationCreator {
public:
  OperationCreatorImpl(const string& name) : OperationCreator(name) {}
  virtual Operation* create() { return new OperationClass; }
};

// Factory class for operations.
class OperationFactory {
public:
  // Register a new creator.
  static void define(const string& name, OperationCreator* creator);
  // Create a new instance of the registered operation.
  static Operation* create(const string& name);
  // Create a new instance of the registered creator. Takes the rhs of the
  // mexFunction and return a new operation object. Caller is responsible for
  // destruction after use.
  static Operation* parse(int nrhs, const mxArray* prhs[]);

private:
  // Obtain a pointer to the registration table.
  static map<string, OperationCreator*>* get_registry();
};

// Definition of an API function. Use with DEFINE_OPERATION. Example:
//
// DECLARE_OPERATION(myfunc);
//
#define DECLARE_OPERATION(name) \
class Operation_##name : public Operation { \
public: \
  virtual void operator()(int nlhs, \
                          mxArray *plhs[], \
                          int nrhs, \
                          const mxArray *prhs[]); \
private: \
  static const OperationCreatorImpl<Operation_##name> creator_; \
};

// Declaration of an API function. Example:
//
// DECLARE_OPERATION(myfunc) {
//   if (nrhs != 1 || nlhs > 1)
//     ERROR("Wrong number of arguments.");
//   ...
// }
//
#define DEFINE_OPERATION(name) \
const OperationCreatorImpl<Operation_##name> \
    Operation_##name::creator_(#name); \
void Operation_##name::operator() (int nlhs, \
                                   mxArray *plhs[], \
                                   int nrhs, \
                                   const mxArray *prhs[])

// API declarations.
DECLARE_OPERATION(open);
DECLARE_OPERATION(close);
DECLARE_OPERATION(get);
DECLARE_OPERATION(put);
DECLARE_OPERATION(delete);
DECLARE_OPERATION(exists);
DECLARE_OPERATION(stat);
DECLARE_OPERATION(keys);
DECLARE_OPERATION(values);

} // namespace bdbmex

#endif // __BDBMEX_H__