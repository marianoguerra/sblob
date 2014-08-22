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
          size=nil,
          % holds the seqnum of the last inserted blob, if it's 0 it means
          % no inserted blob
          seqnum=nil,
          % base_seqnum here is the observed base_seqnum of the first entry
          % the one in config is the one to use in case we are opening a
          % sblob that has no items, that means, if not empty this one will
          % take presedence over the configuration one
          base_seqnum=nil,
          index=nil,
          config=#sblob_cfg{}}).

-record(sblob_entry, {
          % from here the ones written to disk in order
          timestamp,
          seqnum,
          len,
          data,
          % until here, len is written once again after data
          offset,
          size}).

-define(SBLOB_HEADER_SIZE_BYTES, 20).
-define(SBLOB_HEADER_LEN_SIZE_BITS, 32).
-define(SBLOB_HEADER_LEN_SIZE_BYTES, 4).
