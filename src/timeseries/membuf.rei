
type t;

let create : unit => t;
let write : t => string => (int, Ezjsonm.t) => Lwt.t unit;
let read : t => string => Lwt.t (int, Ezjsonm.t);
let length : t => string => Lwt.t int;
let exists : t => string => Lwt.t bool;
let to_list : t => string => Lwt.t (list (int, Ezjsonm.t));
let serialise : t => Lwt.t (list (string, (list (int, Ezjsonm.t))));
let empty : t => Lwt.t unit;
let empty_series : t => string => Lwt.t unit;
let set_disk_range : t => string => option (int, int) => Lwt.t unit;
let get_disk_range : t => string => Lwt.t (option (int, int));
let set_ascending_series : t => string => bool => Lwt.t unit;
let get_ascending_series : t => string => Lwt.t bool;
let set_descending_series : t => string => bool => Lwt.t unit;
let get_descending_series : t => string => Lwt.t bool;