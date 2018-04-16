
type t;

let create : unit => t;
let write : t => string => (int, Ezjsonm.t) => Lwt.t unit;
let read : t => string => Lwt.t (int, Ezjsonm.t);
let length : t => string => Lwt.t int;
let exists : t => string => bool;
let to_list : t => string => Lwt.t (list (int, Ezjsonm.t));
let is_ascending : t => string => int => bool;
let is_descending : t => string => int => bool;
let serialise : t => Lwt.t (list (string, (list (int, Ezjsonm.t))));
let empty : t => Lwt.t unit;
let empty_series : t => string => Lwt.t unit;
let set_disk_range : t => string => option (int, int) => unit;
let get_disk_range : t => string => (option (int, int));