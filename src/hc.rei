type t;

let create : store::Keyvalue.Json.t => t;
let update : ctx::t => info::string => item::Ezjsonm.value => Lwt.t Common.Result.t;
let get : ctx::t => Lwt.t Ezjsonm.t;