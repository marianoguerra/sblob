-record(sblob_cfg, {
          read_ahead=65536, % see file:open read_ahead opton
          max_items=4096,
          max_size_bytes=4194304, % 4MB
          max_age_ms=nil,
          base_seqnum=0}).

-record(sblob, {
          path,
          name,
          fullpath,
          handle=nil,
          position=nil,
          size=0,
          seqnum=0,
          index=nil,
          config=#sblob_cfg{}}).

-record(sblob_entry, {
          timestamp,
          seqnum,
          len,
          size,
          data}).

-define(SBLOB_HEADER_SIZE_BYTES, 20).
-define(SBLOB_HEADER_LEN_SIZE_BITS, 32).
-define(SBLOB_HEADER_LEN_SIZE_BYTES, 4).
