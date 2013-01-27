function delete(varargin)
%BDB.DELETE Delete an entry for a key.
%
%    bdb.delete(key)
%    bdb.delete(id, key)
%
% The function deletes an entry with the given key in the specified
% database session. When the id is omitted, the default session is used.
%
% The key must be an ordinary object.
%
% See also bdb.put bdb.get
  driver_('del', varargin{:});
end
