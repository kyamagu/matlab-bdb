function make(varargin)
%BDB.MAKE Build a driver mex file.
%
%    bdb.make(['optionName', optionValue,] [compiler_flags])
%
% bdb.make builds a mex file for the bdb driver. The make script
% accepts db path option and compiler flags for the mex command.
% See below for the supported build options.
%
% The libdb must be installed in the system. Also, for data compression,
% zlib library is required.
%
% Options:
%
%    Option name     Value
%    --------------- -------------------------------------------------
%    --libdb_path    path to libdb.a. e.g., /usr/lib/libdb.a
%    --libz_path     path to libz.a. e.g., /usr/lib/libz.a
%    --enable_zlib   true or false (default true)
%
% By default, db.make looks for a system library path for dynamic linking.
%
% The enable_zlib flag specifies whether to compress values in the
% database. Compression can significantly save disk space when the data
% consists of repetitive values such as a big zero array. However, when
% data is near random, almost no saving in storage space with slower
% storage access. Note that the database file created with compression
% enabled is not compatible with the driver built without the compression
% flag, or vice versa. By default, compression is turned on.
%
% Example:
%
% Disable ZLIB compression.
%
% >> bdb.make('--enable_zlib', false);
%
% Specifying additional paths.
%
% >> bdb.make('-I/opt/local/include', '-L/opt/local/lib');
%
% Specifying library files.
%
% >> bdb.make('--libdb_path', '/opt/local/lib/libdb.a', ...
%            '-I/opt/local/include');
%
% See also mex

    package_dir = fileparts(mfilename('fullpath'));
    [config, compiler_flags] = parse_options(varargin{:});
    cmd = sprintf(...
        'mex -largeArrayDims %s %s -outdir %s -output driver_ %s %s%s',...
        fullfile(package_dir, 'private', 'driver.cc'),...
        fullfile(package_dir, 'private', 'bdbmex.cc'),...
        fullfile(package_dir, 'private'),...
        config.db_path,...
        repmat(['-DENABLE_ZLIB ', config.zlib_path], 1, config.enable_zlib),...
        compiler_flags...
        );
    disp(cmd);
    eval(cmd);

end

function [config, compiler_flags] = parse_options(varargin)
%PARSE_OPTIONS Parse build options.

    config.db_path = '-ldb';
    config.zlib_path = '-lz';
    config.enable_zlib = true;
    mark_for_delete = false(size(varargin));
    for i = 1:2:numel(varargin)
        if strcmp(varargin{i}, '--libdb_path')
            config.db_path = varargin{i+1};
            mark_for_delete(i:i+1) = true;
        end
        if strcmp(varargin{i}, '--libz_path')
            config.zlib_path = varargin{i+1};
            mark_for_delete(i:i+1) = true;
        end
        if strcmp(varargin{i}, '--enable_zlib')
            config.enable_zlib = logical(varargin{i+1});
            mark_for_delete(i:i+1) = true;
        end
    end
    compiler_flags = sprintf(' %s', varargin{~mark_for_delete});

end
