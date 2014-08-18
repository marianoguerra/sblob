-record(sblob_cfg, {
          read_ahead=65536 % see file:open read_ahead opton
         }).

-record(sblob, {
          path,
          name,
          fullpath,
          handle=nil,
          position=nil,
          seqnum=0,
          config=#sblob_cfg{}}).

-record(sblob_entry, {
          timestamp,
          seqnum,
          len,
          data}).

-define(SBLOB_HEADER_SIZE_BYTES, 20).
-define(SBLOB_HEADER_LEN_SIZE_BITS, 32).
-define(SBLOB_HEADER_LEN_SIZE_BYTES, 4).
