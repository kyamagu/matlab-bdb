// Berkeley DB matlab driver library.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#ifdef ENABLE_ZLIB
#include <zlib.h>
#endif
#include "bdbmex.h"

namespace bdbmex {

Record::Record() {
  reset(DB_DBT_REALLOC, DB_DBT_REALLOC);
}

Record::Record(const string& key) {
  reset(DB_DBT_USERMEM, DB_DBT_REALLOC);
  set_key(key);
}

Record::Record(const string& key, const mxArray* value) {
  reset(DB_DBT_USERMEM, DB_DBT_USERMEM);
  set_key(key);
  set_value(value);
}

Record::~Record() {
  if (key_.flags == DB_DBT_REALLOC && key_.data)
    free(key_.data);
  if (value_.flags == DB_DBT_REALLOC && value_.data)
    free(value_.data);
}

void Record::reset(u_int32_t key_flags, u_int32_t value_flags) {
  memset(&key_, 0, sizeof(DBT));
  memset(&value_, 0, sizeof(DBT));
  key_.flags = key_flags;
  value_.flags = value_flags;
}

void Record::set_key(const string& key) {
  key_.data = static_cast<void*>(const_cast<char*>(key.c_str()));
  key_.size = key.size() + 1;
}

void Record::set_value(const mxArray* value) {
  serialize_mxarray(value, &buffer_);
  value_.data = &buffer_[0];
  value_.size = buffer_.size();
}

void Record::get_key(string* key) {
  key->assign(static_cast<const char*>(key_.data));
}

void Record::get_value(mxArray** value) {
  const uint8_t* value_data = static_cast<const uint8_t*>(value_.data);
  buffer_.assign(value_data, value_data + value_.size);
  deserialize_mxarray(buffer_, value);
}

#ifdef ENABLE_ZLIB

void Record::serialize_mxarray(const mxArray* value, vector<uint8_t>* binary) {
  mxArray* serialzed_array = static_cast<mxArray*>(mxSerialize(value));
  uLongf array_size = mxGetNumberOfElements(serialzed_array);
  vector<uint8_t> buffer(compressBound(array_size));
  uLongf actual_size = buffer.size();
  int code_ = compress(&buffer[0],
                       &actual_size,
                       static_cast<const Bytef*>(mxGetData(serialzed_array)),
                       array_size);
  binary->resize(sizeof(uLongf) + actual_size);
  memcpy(&(*binary)[0], &array_size, sizeof(uLongf));
  copy(buffer.begin(),
       buffer.begin() + actual_size,
       binary->begin() + sizeof(uLongf));
  mxDestroyArray(serialzed_array);
  if (code_ != Z_OK)
    ERROR("Fatal error in compress_mxarray");
}

void Record::deserialize_mxarray(const vector<uint8_t>& binary,
                                mxArray** value) {
  if (binary.size() <= sizeof(uint32_t))
    ERROR("Fatal error in decompress_mxarray: invalid binary.");
  uLongf array_size = 0;
  memcpy(&array_size, &binary[0], sizeof(uLongf));
  vector<uint8_t> buffer(array_size);
  uLongf actual_size = array_size;
  int code_ = uncompress(&buffer[0],
                         &actual_size,
                         &binary[0] + sizeof(uLongf),
                         binary.size() - sizeof(uLongf));
  *value = static_cast<mxArray*>(mxDeserialize(&buffer[0], buffer.size()));
  if (code_ != Z_OK)
    ERROR("Fatal error in decompress_mxarray: code = %d.", code_);
}

#else

void Record::serialize_mxarray(const mxArray* value, vector<uint8_t>* binary) {
  mxArray* serialzed_array = static_cast<mxArray*>(mxSerialize(value));
  const uint8_t* data = static_cast<uint8_t*>(mxGetData(serialzed_array));
  binary->assign(data, data + mxGetNumberOfElements(serialzed_array));
  mxDestroyArray(serialzed_array);
}

void Record::deserialize_mxarray(const vector<uint8_t>& binary,
                                 mxArray** value) {
  *value = static_cast<mxArray*>(mxDeserialize(&binary[0], binary.size()));
}

#endif

Database::Database() : code_(0), database_(NULL) {}

Database::~Database() {
  close();
}

bool Database::open(const string& filename) {
  code_ = db_create(&database_, NULL, 0);
  if (!ok()) return false;
  code_ = database_->open(database_,
                          NULL, filename.c_str(),
                          NULL,
                          DB_BTREE,
                          DB_CREATE,
                          0);
  return ok();
}

void Database::close() {
  if (database_) {
    database_->close(database_, 0);
    database_ = NULL;
  }
}

int Database::error_code() {
  return code_;
}

const char* Database::error_message() {
  return db_strerror(code_);
}

bool Database::get(const string& key, mxArray** value) {
  Record record(key);
  code_ = database_->get(database_, NULL, record.key(), record.value(), 0);
  if (code_ == 0)
    record.get_value(value);
  else if (code_ == DB_NOTFOUND)
    *value = mxCreateDoubleMatrix(0, 0, mxREAL);
  return ok() || (code_ == DB_NOTFOUND);
}

bool Database::put(const string& key, const mxArray* value) {
  Record record(key, value);
  code_ = database_->put(database_, NULL, record.key(), record.value(), 0);
  return ok();
}

bool Database::del(const string& key) {
  Record record(key);
  code_ = database_->del(database_, NULL, record.key(), 0);
  return ok();
}

bool Database::exists(const string& key, mxArray** value) {
  Record record(key);
  code_ = database_->exists(database_, NULL, record.key(), 0);
  *value = mxCreateLogicalScalar(ok());
  return ok() || code_ == DB_NOTFOUND;
}

bool Database::stat(mxArray** output) {
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  const char* kFields[] = {"magic", "minkey", "ndata", "nkeys",
      "pagecnt", "pagesize", "re_len", "re_pad", "version"};
  code_ = database_->stat(database_, NULL, &stats, 0);
  *output = mxCreateStructMatrix(1, 1, 9, kFields);
  mxSetField(*output, 0, kFields[0], mxCreateDoubleScalar(stats->bt_magic));
  mxSetField(*output, 0, kFields[1], mxCreateDoubleScalar(stats->bt_minkey));
  mxSetField(*output, 0, kFields[2], mxCreateDoubleScalar(stats->bt_ndata));
  mxSetField(*output, 0, kFields[3], mxCreateDoubleScalar(stats->bt_nkeys));
  mxSetField(*output, 0, kFields[4], mxCreateDoubleScalar(stats->bt_pagecnt));
  mxSetField(*output, 0, kFields[5], mxCreateDoubleScalar(stats->bt_pagesize));
  mxSetField(*output, 0, kFields[6], mxCreateDoubleScalar(stats->bt_re_len));
  mxSetField(*output, 0, kFields[7], mxCreateDoubleScalar(stats->bt_re_pad));
  mxSetField(*output, 0, kFields[8], mxCreateDoubleScalar(stats->bt_version));
  free(stats);
  return ok();
}

bool Database::keys(mxArray** output) {
  // Count the number of keys.
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  code_ = database_->stat(database_, NULL, &stats, 0);
  uint32_t num_keys = stats->bt_nkeys;
  free(stats);
  if (code_)
    return false;
  // Retrieve records.
  Cursor cursor;
  code_ = cursor.open(database_);
  if (code_)
    return false;
  *output = mxCreateCellMatrix(num_keys, 1);
  int index = 0;
  Record record;
  while (index < num_keys && 0 == (code_ = cursor.next(&record))) {
    string key;
    record.get_key(&key);
    mxSetCell(*output, index++, mxCreateString(key.c_str()));
  }
  return ok() || (code_ == DB_NOTFOUND);
}

bool Database::values(mxArray** output) {
  // Count the number of values.
  DB_BTREE_STAT* stats = NULL;
  stats = static_cast<DB_BTREE_STAT*>(malloc(sizeof(DB_BTREE_STAT)));
  code_ = database_->stat(database_, NULL, &stats, 0);
  uint32_t num_values = stats->bt_ndata;
  free(stats);
  if (code_)
    return false;
  // Retrieve records.
  Cursor cursor;
  code_ = cursor.open(database_);
  if (code_)
    return false;
  *output = mxCreateCellMatrix(num_values, 1);
  int index = 0;
  Record record;
  while (index < num_values && 0 == (code_ = cursor.next(&record))) {
    mxArray* value_array;
    record.get_value(&value_array);
    mxSetCell(*output, index++, value_array);
  }
  return ok() || (code_ == DB_NOTFOUND);
}

DatabaseManager::DatabaseManager() : last_id_(0) {}

DatabaseManager::~DatabaseManager() {}

int DatabaseManager::open(const string& filename) {
  if (!connections_[++last_id_].open(filename))
    ERROR("Unable to open: %s.", filename.c_str());
  return last_id_;
}

void DatabaseManager::close(int id) {
  connections_.erase(id);
}

int DatabaseManager::default_id() const {
  return (connections_.empty()) ? 0 : connections_.begin()->first;
}

int DatabaseManager::last_id() const {
  return last_id_;
}

Database* DatabaseManager::get(int id) {
  map<int, Database>::iterator connection = connections_.find(id);
  if (connection == connections_.end())
    ERROR("Invalid session id: %d. Did you open the database?", id);
  return &connection->second;
}

DatabaseManager Operation::manager_ = DatabaseManager();

Operation* Operation::parse(int nrhs, const mxArray *prhs[]) {
  const vector<const mxArray*> input(prhs, prhs + nrhs);
  auto_ptr<Operation> operation;
  PARSER_STATE state = PARSER_INIT;
  int id = 0;
  vector<const mxArray*>::const_iterator it = input.begin();
  while (state != PARSER_FINISH) {
    switch (state) {
      case PARSER_INIT: {
        if (it == input.end())
          ERROR("Invalid argument: empty argument.");
        else if (mxIsChar(*it)) {
          string arg(mxGetChars(*it),
                     mxGetChars(*it) + mxGetNumberOfElements(*it));
          if (arg == "open") {
            operation.reset(new OpenOperation());
            ++it;
          }
          else if (arg == "close") {
            operation.reset(new CloseOperation());
            ++it;
          }
          else if (arg == "get") {
            operation.reset(new GetOperation());
            ++it;
          }
          else if (arg == "put") {
            operation.reset(new PutOperation());
            ++it;
          }
          else if (arg == "del") {
            operation.reset(new DelOperation());
            ++it;
          }
          else if (arg == "exists") {
            operation.reset(new ExistsOperation());
            ++it;
          }
          else if (arg == "stat") {
            operation.reset(new StatOperation());
            ++it;
          }
          else if (arg == "keys") {
            operation.reset(new KeysOperation());
            ++it;
          }
          else if (arg == "values") {
            operation.reset(new ValuesOperation());
            ++it;
          }
          else // operation omitted.
            ERROR("Invalid argument: missing operation.");
        }
        else
          ERROR("Invalid argument: operation is not char.");
        state = PARSER_CMD;
        break;
      }
      case PARSER_CMD: {
        if (it != input.end() && 
            mxIsDouble(*it) &&
            mxGetNumberOfElements(*it) == 1) {
          id = mxGetScalar(*it);
          ++it;
        }
        else
          id = manager()->default_id();
        operation->id_ = id;
        state = PARSER_ID;
        break;
      }
      case PARSER_ID: {
        operation->args_.assign(it, input.end());
        state = PARSER_FINISH;
        break;
      }
      case PARSER_FINISH: {
        ERROR("Fatal error.");
        break;
      }
    }
  }
  return operation.release();
}

void OpenOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  const vector<const mxArray*>& args = arguments();
  if (args.size() != 1 || !mxIsChar(args[0]))
    ERROR("Failed to parse filename.");
  string filename(mxGetChars(args[0]),
                  mxGetChars(args[0]) + mxGetNumberOfElements(args[0]));
  plhs[0] = mxCreateDoubleScalar(manager()->open(filename));
}

void CloseOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs)
    ERROR("Too many output: %d for 0.", nlhs);
  if (arguments().size() != 0)
    ERROR("Too many input.");
  manager()->close(id());
}

void GetOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  const vector<const mxArray*>& args = arguments();
  if (args.size() != 1)
    ERROR("Too many input: %d for 0.", args.size());
  if (!mxIsChar(args[0]))
    ERROR("Invalid key.");
  Database* connection = manager()->get(id());
  string key(mxGetChars(args[0]),
             mxGetChars(args[0]) + mxGetNumberOfElements(args[0]));
  if (!connection->get(key, &plhs[0]))
    ERROR("Failed to get a key: %s", key.c_str());
}

void PutOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 0)
    ERROR("Too many output: %d for 1.", nlhs);
  const vector<const mxArray*>& args = arguments();
  if (args.size() != 2)
    ERROR("Wrong number of arguments: %d for 2.", args.size());
  if (!mxIsChar(args[0]))
    ERROR("Invalid key.");
  Database* connection = manager()->get(id());
  string key(mxGetChars(args[0]),
             mxGetChars(args[0]) + mxGetNumberOfElements(args[0]));
  if (!connection->put(key, args[1]))
    ERROR("Failed to put a key: %s", key.c_str());
}

void DelOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 0)
    ERROR("Too many output: %d for 1.", nlhs);
  const vector<const mxArray*>& args = arguments();
  if (args.size() != 1 || !mxIsChar(args[0]))
    ERROR("Invalid key input.");
  Database* connection = manager()->get(id());
  string key(mxGetChars(args[0]),
             mxGetChars(args[0]) + mxGetNumberOfElements(args[0]));
  if (!connection->del(key))
    ERROR("Failed to delete a key: %s", key.c_str());
}

void ExistsOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  const vector<const mxArray*>& args = arguments();
  if (args.size() != 1 || !mxIsChar(args[0]))
    ERROR("Invalid key input.");
  Database* connection = manager()->get(id());
  string key(mxGetChars(args[0]),
             mxGetChars(args[0]) + mxGetNumberOfElements(args[0]));
  if (!connection->exists(key, &plhs[0]))
    ERROR("Failed to query a key: %s", key.c_str());
}

void StatOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (arguments().size() > 0)
    ERROR("Too many input: %d for 0.", arguments().size());
  Database* connection = manager()->get(id());
  if (!connection->stat(&plhs[0]))
    ERROR("%s", connection->error_message());
}

void KeysOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (arguments().size() > 0)
    ERROR("Too many input: %d for 0.", arguments().size());
  Database* connection = manager()->get(id());
  if (!connection->keys(&plhs[0]))
    ERROR("%s", connection->error_message());
}

void ValuesOperation::run(int nlhs, mxArray* plhs[]) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (arguments().size() > 0)
    ERROR("Too many input: %d for 0.", arguments().size());
  Database* connection = manager()->get(id());
  if (!connection->values(&plhs[0]))
    ERROR("%s", connection->error_message());
}

} // namespace dbmex