type t;

let create : unit => t;
let is_observed : t => (string, int) => bool;
let expire : t => Lwt.t (list (string, Int32.t, string));
let add : t => string => Prov.t => Lwt.t unit;
let get : t => (string, int) => (list (string, Int32.t, string));
let get_all : t => (list (string, Int32.t, string));