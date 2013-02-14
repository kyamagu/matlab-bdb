// Berkeley DB mex interface.
//
// Kota Yamaguchi 2012 <kyamagu@cs.stonybrook.edu>
#include "bdbmex.h"

using bdbmex::Operation;
using bdbmex::OperationFactory;

// Entry point to the mex function.
void mexFunction(int nlhs, mxArray *plhs[], int nrhs, const mxArray *prhs[]){
  auto_ptr<Operation> operation(OperationFactory::parse(nrhs, prhs));
  (*operation)(nlhs, plhs, nrhs - 1, prhs + 1);
}