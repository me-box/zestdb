type t;
let create : file::string => t;
let get : t => string => Lwt.t Ezjsonm.t;
let alist : t => string => Lwt.t (list string);
let count : t => string => Lwt.t int;
let update : t => string => string => string => Lwt.t unit;
let delete : t => string => string => string => Lwt.t unit;