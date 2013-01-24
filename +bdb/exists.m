function value = exists(varargin)
%BDB.EXISTS Check if an entry exists.
%
%    flag = bdb.exists(key)
%    flag = bdb.exists(id, key)
%
% The function checks if an entry with the given key exists in the specified
% database session. When the id is omitted, the default session is used.
%
% The key must be a char array.
%
% See also bdb.get
  value = driver_('exists', varargin{:});
end