type t;

let create: unit => t;

let push: (t, (int, Ezjsonm.t)) => unit;

let pop: t => (int, Ezjsonm.t);

let length: t => int;

let to_list: t => list((int, Ezjsonm.t));

let is_ascending: (t, int) => bool;

let is_descending: (t, int) => bool;

let clear: t => unit;

let set_disk_range: (t, option((int, int))) => unit;

let get_disk_range: t => option((int, int));