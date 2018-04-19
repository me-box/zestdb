type t;
let create : file::string => t;
let get : t => string => Lwt.t Ezjsonm.t;
let update : t => string => string => Lwt.t unit;
let delete : t => string => string => Lwt.t unit;