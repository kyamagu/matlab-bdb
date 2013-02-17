function id = open(filename, varargin)
%OPEN Open a Berkeley DB database.
%
%    id = bdb.open(filename)
%    id = bdb.open(filename, home_dir)
%
% The function opens the database session for the given db file. Optionally
% it takes a path to the environment directory for transactions.
%
% See also bdb.close bdb.put bdb.get bdb.delete bdb.stat bdb.keys
% bdb.values
  id = mex_function_(mfilename, filename, varargin{:});
end
