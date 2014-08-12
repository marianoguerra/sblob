-record(sblob_cfg, {
          read_ahead=65536 % see file:open read_ahead opton
         }).

-record(sblob, {
          path,
          name,
          fullpath,
          handle=nil,
          seqnum=0,
          config=#sblob_cfg{}}).

-record(sblob_entry, {
          timestamp,
          seqnum,
          data}).
