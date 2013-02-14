// Berkeley DB matlab driver operations.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#include "bdbmex.h"

namespace bdbmex {

DEFINE_OPERATION(open) {
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs < 1 || nrhs > 2)
    ERROR("Wrong number of arguments: %d for 1 or 2.", nrhs);
  if (!mxIsChar(prhs[0]) || (nrhs == 2 && !mxIsChar(prhs[1])))
    ERROR("Invalid filename is given.");
  string filename(mxGetChars(prhs[0]),
                  mxGetChars(prhs[0]) + mxGetNumberOfElements(prhs[0]));
  string home_dir((nrhs == 1) ? "" : string(mxGetChars(prhs[1]),
      mxGetChars(prhs[1]) + mxGetNumberOfElements(prhs[1])));
  plhs[0] = mxCreateDoubleScalar(sessions()->open(filename, home_dir));
}

DEFINE_OPERATION(close) {
  if (nlhs)
    ERROR("Too many output: %d for 0.", nlhs);
  if (nrhs > 1 || (nrhs == 1 && !mxIsNumeric(prhs[0])))
    ERROR("Invalid input.");
  int id = (nrhs == 0) ? sessions()->default_id() : mxGetScalar(prhs[0]);
  sessions()->close(id);
}

DEFINE_OPERATION(get) {
  const mxArray* key = NULL;
  Database* connection = NULL;
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  if (nrhs == 1) {
    connection = sessions()->get(sessions()->default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = sessions()->get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->get(key, &plhs[0]))
    ERROR("Failed to get an entry: %s", connection->error_message());
}

DEFINE_OPERATION(put) {
  const mxArray* key = NULL;
  const mxArray* value = NULL;
  Database* connection = NULL;
  if (nlhs > 0)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 3)
    ERROR("Too many input: %d for 2 or 3.", nrhs);
  if (nrhs == 2) {
    connection = sessions()->get(sessions()->default_id());
    key = prhs[0];
    value = prhs[1];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = sessions()->get(mxGetScalar(prhs[0]));
    key = prhs[1];
    value = prhs[2];
  }
  else
    ERROR("Invalid input.");
  if (!connection->put(key, value))
    ERROR("Failed to put an entry: %s", connection->error_message());
}

DEFINE_OPERATION(delete) {
  const mxArray* key = NULL;
  Database* connection = NULL;
  if (nlhs > 0)
    ERROR("Too many output: %d for 0.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  if (nrhs == 1) {
    connection = sessions()->get(sessions()->default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = sessions()->get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->del(key))
    ERROR("Failed to delete an entry: %s", connection->error_message());
}

DEFINE_OPERATION(exists) {
  const mxArray* key = NULL;
  Database* connection = NULL;
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 2)
    ERROR("Too many input: %d for 1 or 2.", nrhs);
  if (nrhs == 1) {
    connection = sessions()->get(sessions()->default_id());
    key = prhs[0];
  }
  else if (mxIsNumeric(prhs[0])) {
    connection = sessions()->get(mxGetScalar(prhs[0]));
    key = prhs[1];
  }
  else
    ERROR("Invalid input.");
  if (!connection->exists(key, &plhs[0]))
    ERROR("Failed to query a key: %s", connection->error_message());
}

DEFINE_OPERATION(stat) {
  Database* connection = NULL;
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  int id = 0;
  if (nrhs == 0)
    id = sessions()->default_id();
  else if (mxIsNumeric(prhs[0]))
    id = mxGetScalar(prhs[0]);
  else
    ERROR("Invalid input.");
  connection = sessions()->get(id);
  if (!connection->stat(&plhs[0]))
    ERROR("Failed to query stat: %s", connection->error_message());
}

DEFINE_OPERATION(keys) {
  Database* connection = NULL;
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  int id = 0;
  if (nrhs == 0)
    id = sessions()->default_id();
  else if (mxIsNumeric(prhs[0]))
    id = mxGetScalar(prhs[0]);
  else
    ERROR("Invalid input.");
  connection = sessions()->get(id);
  if (!connection->keys(&plhs[0]))
    ERROR("Failed to query keys: %s", connection->error_message());
}

DEFINE_OPERATION(values) {
  Database* connection = NULL;
  if (nlhs > 1)
    ERROR("Too many output: %d for 1.", nlhs);
  if (nrhs > 1)
    ERROR("Too many input: %d for 0 or 1.", nrhs);
  int id = 0;
  if (nrhs == 0)
    id = sessions()->default_id();
  else if (mxIsNumeric(prhs[0]))
    id = mxGetScalar(prhs[0]);
  else
    ERROR("Invalid input.");
  connection = sessions()->get(id);
  if (!connection->values(&plhs[0]))
    ERROR("Failed to query values: %s", connection->error_message());
}

} // namespace dbmex