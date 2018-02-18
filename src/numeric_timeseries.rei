include (module type of Timeseries);

let read_last : ctx::t => id::string => n::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_latest : ctx::t => id::string => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_first : ctx::t => id::string => n::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_earliest : ctx::t => id::string => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_since : ctx::t => id::string => from::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;
let read_range : ctx::t => id::string => from::int => to::int => fn::list (Ezjsonm.t => Ezjsonm.t) => Lwt.t Ezjsonm.t;

