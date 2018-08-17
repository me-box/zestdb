type t;
let create: unit => t;
let add: (t, string) => unit;
let get_all: t => list(string);