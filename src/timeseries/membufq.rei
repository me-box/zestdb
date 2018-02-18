
type t;

let create : unit => t;
let push : t => (int, Ezjsonm.t) => unit;
let pop : t => (int, Ezjsonm.t);
let length : t => int;
let to_list : t => list (int, Ezjsonm.t);
let clear : t => unit;
let set_disk_range : t => option (int, int) => unit;
let get_disk_range : t => option (int, int);
let set_ascending_series : t => bool => unit;
let get_ascending_series : t => bool;
let set_descending_series : t => bool => unit;
let get_descending_series : t => bool;