include (module type of Timeseries);

let create : path_to_db::string => max_buffer_size::int => shard_size::int => t;
let read_last : ctx::t => id_list::list string => n::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_latest : ctx::t => id_list::list string => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_first : ctx::t => id_list::list string => n::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_earliest : ctx::t => id_list::list string => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_since : ctx::t => id_list::list string => from::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_range : ctx::t => id_list::list string => from::int => to::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let is_valid : Ezjsonm.value => bool;
