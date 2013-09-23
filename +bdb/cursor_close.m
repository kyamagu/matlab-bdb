function cursor_close(cursor_id)
%CURSOR_CLOSE Close a cursor.
%
%    bdb.cursor_close(cursor_id)
%
% The function closes a cursor.
%
% See also bdb.cursor_open
  libbdb(mfilename, cursor_id);
end
