include Timeseries;

open Lwt.Infix;

let create path_to_db::path max_buffer_size::mbs shard_size::ss => {
  Timeseries.create path_to_db::(path ^ __MODULE__) max_buffer_size::mbs shard_size::ss;
};