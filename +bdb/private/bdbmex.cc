/// Berkeley DB mex interface.
///
/// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>

#include "libbdbmex.h"
#include "mex/function.h"
#include "mex/mxarray.h"

using mex::CheckInputArguments;
using mex::CheckOutputArguments;
using mex::MxArray;
using bdbmex::Sessions;
using bdbmex::Database;
using bdbmex::Cursor;

MEX_FUNCTION(open) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  string filename(MxArray(prhs[0]).toString());
  string home_dir((nrhs == 1) ? "" : MxArray(prhs[1]).toString());
  plhs[0] = MxArray(Sessions::open(filename, home_dir)).getMutable();
}

MEX_FUNCTION(close) (int nlhs,
                     mxArray *plhs[],
                     int nrhs,
                     const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  int id = (nrhs == 0) ? Sessions::default_id() : MxArray(prhs[0]).toInt();
  Sessions::close(id);
}

MEX_FUNCTION(get) (int nlhs,
                   mxArray *plhs[],
                   int nrhs,
                   const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->get(key.get(), &plhs[0]))
    ERROR("Failed to get an entry: %s", connection->error_message());
}

MEX_FUNCTION(put) (int nlhs,
                   mxArray *plhs[],
                   int nrhs,
                   const mxArray *prhs[]) {
  CheckInputArguments(2, 3, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Database* connection = NULL;
  MxArray key, value;
  if (nrhs == 2) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
    value.reset(prhs[1]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
    value.reset(prhs[2]);
  }
  if (!connection->put(key.get(), value.get()))
    ERROR("Failed to put an entry: %s", connection->error_message());
}

MEX_FUNCTION(delete) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->del(key.get()))
    ERROR("Failed to delete an entry: %s", connection->error_message());
}

MEX_FUNCTION(exists) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(1, 2, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  MxArray key;
  if (nrhs == 1) {
    connection = Sessions::get(Sessions::default_id());
    key.reset(prhs[0]);
  }
  else {
    connection = Sessions::get(MxArray(prhs[0]).toInt());
    key.reset(prhs[1]);
  }
  if (!connection->exists(key.get(), &plhs[0]))
    ERROR("Failed to query a key: %s", connection->error_message());
}

MEX_FUNCTION(stat) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->stat(&plhs[0]))
    ERROR("Failed to query stat: %s", connection->error_message());
}

MEX_FUNCTION(keys) (int nlhs,
                    mxArray *plhs[],
                    int nrhs,
                    const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->keys(&plhs[0]))
    ERROR("Failed to query keys: %s", connection->error_message());
}

MEX_FUNCTION(values) (int nlhs,
                      mxArray *plhs[],
                      int nrhs,
                      const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->values(&plhs[0]))
    ERROR("Failed to query values: %s", connection->error_message());
}

MEX_FUNCTION(compact) (int nlhs,
                       mxArray *plhs[],
                       int nrhs,
                       const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Database* connection = NULL;
  if (nrhs == 0)
    connection = Sessions::get(Sessions::default_id());
  else
    connection = Sessions::get(MxArray(prhs[0]).toInt());
  if (!connection->compact())
    ERROR("Failed to compact: %s", connection->error_message());
}

MEX_FUNCTION(sessions) (int nlhs,
                        mxArray *plhs[],
                        int nrhs,
                        const mxArray *prhs[]) {
  CheckInputArguments(0, 0, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  const map<int, Database>& connections = Sessions::connections();
  vector<int> session_ids;
  session_ids.reserve(connections.size());
  for (map<int, Database>::const_iterator it = connections.begin();
       it != connections.end(); ++it)
    session_ids.push_back(it->first);
  plhs[0] = MxArray(session_ids).getMutable();
}

MEX_FUNCTION(cursor_open) (int nlhs,
                           mxArray *plhs[],
                           int nrhs,
                           const mxArray *prhs[]) {
  CheckInputArguments(0, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  int id = (nrhs == 0) ? Sessions::default_id() : MxArray(prhs[0]).toInt();
  plhs[0] = MxArray(Sessions::open_cursor(id)).getMutable();
}

MEX_FUNCTION(cursor_close) (int nlhs,
                            mxArray *plhs[],
                            int nrhs,
                            const mxArray *prhs[]) {
  CheckInputArguments(1, 1, nrhs);
  CheckOutputArguments(0, 0, nlhs);
  Sessions::close_cursor(MxArray(prhs[0]).toInt());
}

MEX_FUNCTION(cursor_next) (int nlhs,
                           mxArray *plhs[],
                           int nrhs,
                           const mxArray *prhs[]) {
  CheckInputArguments(1, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Cursor* cursor = Sessions::get_cursor(MxArray(prhs[0]).toInt());
  int code = cursor->next();
  if (code == 0)
    plhs[0] = MxArray(true).getMutable();
  else if (code == DB_NOTFOUND)
    plhs[0] = MxArray(false).getMutable();
  else
    ERROR("Failed to move a cursor: %s", cursor->error_message());
}

MEX_FUNCTION(cursor_prev) (int nlhs,
                           mxArray *plhs[],
                           int nrhs,
                           const mxArray *prhs[]) {
  CheckInputArguments(1, 1, nrhs);
  CheckOutputArguments(0, 1, nlhs);
  Cursor* cursor = Sessions::get_cursor(MxArray(prhs[0]).toInt());
  int code = cursor->prev();
  if (code == 0)
    plhs[0] = MxArray(true).getMutable();
  else if (code == DB_NOTFOUND)
    plhs[0] = MxArray(false).getMutable();
  else
    ERROR("Failed to move a cursor: %s", cursor->error_message());
}

MEX_FUNCTION(cursor_get) (int nlhs,
                          mxArray *plhs[],
                          int nrhs,
                          const mxArray *prhs[]) {
  CheckInputArguments(1, 1, nrhs);
  CheckOutputArguments(0, 2, nlhs);
  Cursor* cursor = Sessions::get_cursor(MxArray(prhs[0]).toInt());
  if (cursor->error_code() != 0)
    ERROR("Failed to get from cursor: %s", cursor->error_message());
  cursor->get()->get_key(&plhs[0]);
  if (nlhs > 1)
    cursor->get()->get_value(&plhs[1]);
}