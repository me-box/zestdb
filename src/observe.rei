type t;

let create : unit => t;
let update : t => list ((string, int), list (string, Int32.t)) => unit;
let unwrap : t => list ((string, int), list (string, Int32.t));
let list_uuids : list ((string, int), list (string, Int32.t)) => list (string, Int32.t);
let expire : t => Int32.t => list ((string, int), list (string, Int32.t));
let has_observed : array (int, 'a) => bool;
let is_observed : t => (string, int) => bool;
let observed_paths_exist : t => bool;
let get_ident : t => (string, int) => list (string, Int32.t);
let add_to_observe : t => string => int => string => Int32.t => Lwt.t unit;
let diff : list (string, Int32.t) => list (string, Int32.t) => list (string, Int32.t);