type t;
let create : file::string => Lwt.t t;
let get : Lwt.t t => string => Lwt.t Ezjsonm.t;
let update : Lwt.t t => string => string => Lwt.t unit;