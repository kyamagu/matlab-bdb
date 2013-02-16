function close(varargin)
%BDB.CLOSE Close the database.
%
%    bdb.close
%    bdb.close(id)
%
% The function closes the database session of the specified id. When the id is
% omitted, the default session is closed.
%
% See also bdb.open
  bdbmex_(mfilename, varargin{:});
end
