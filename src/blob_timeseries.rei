include (module type of Timeseries);

let create: (~path_to_db: string, ~max_buffer_size: int, ~shard_size: int) => t;