-record(gblob_cfg, {
          read_ahead=65536, % see file:open read_ahead opton
          max_items=4096,
          max_size_bytes=4194304, % 4MB
          max_age_ms=nil}).

-record(gblob, {
          % path to gblob folder
          path,
          % current sblob, will be opened only when required
          current=nil,
          min_chunk_num=0,
          max_chunk_num=0,
          config=#gblob_cfg{}}).
