type t;
let create : file::string => Lwt.t t;
let update : Lwt.t t => string => string => (int, int) => list (int, int)  => Lwt.t (option (int, int));
let get : Lwt.t t => string => Lwt.t (option (list (int, int)));
let overlap : Lwt.t t => string => (int, int) => Lwt.t (list (int, int));
let range : Lwt.t t => string => Lwt.t (option (int, int));