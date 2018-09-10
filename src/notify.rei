type t;
let create: unit => t;
let add: (t, string) => bool;
let get_all: t => list(string);