
type t;
let create : file::string => Lwt.t t;
let add : Lwt.t t => list string => list (int, Ezjsonm.t) => string => Lwt.t unit;
let get : Lwt.t t => list string => Lwt.t (list (int, Ezjsonm.t));
let remove : Lwt.t t => list (list string) => string => Lwt.t unit;
