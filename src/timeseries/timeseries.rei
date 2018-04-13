type t;

let create : path_to_db::string => max_buffer_size::int => shard_size::int => t;
let flush : ctx::t => Lwt.t unit;
let write : ctx::t => timestamp::option int ? => id::string => json::Ezjsonm.t => Lwt.t unit;
let read_last : ctx::t => id::string => n::int => Lwt.t Ezjsonm.t;
let read_lasts : ctx::t => id_list::list string => n::int => Lwt.t Ezjsonm.t;
let read_latest : ctx::t => id::string => Lwt.t Ezjsonm.t;
let read_latests : ctx::t => id_list::list string => Lwt.t Ezjsonm.t;
let read_first : ctx::t => id::string => n::int => Lwt.t Ezjsonm.t;
let read_firsts : ctx::t => id_list::list string => n::int => Lwt.t Ezjsonm.t;
let read_since : ctx::t => id::string => from::int => Lwt.t Ezjsonm.t;
let read_range : ctx::t => id::string => from::int => to::int => Lwt.t Ezjsonm.t;
let read_earliest : ctx::t => id::string => Lwt.t Ezjsonm.t;
let read_earliests : ctx::t => id_list::list string => Lwt.t Ezjsonm.t;
let length : ctx::t => id::string => Lwt.t Ezjsonm.t;
let lengths : ctx::t => id_list::list string => Lwt.t Ezjsonm.t;