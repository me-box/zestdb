include Timeseries;

open Lwt.Infix;

let create = (~path_to_db as path, ~max_buffer_size as mbs, ~shard_size as ss) =>
  Timeseries.create(~path_to_db=path ++ __MODULE__, ~max_buffer_size=mbs, ~shard_size=ss);