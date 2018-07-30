type t;

let create: (~file: string) => Lwt.t(t);

let add: (Lwt.t(t), string, list(string), list((int, Ezjsonm.t))) => Lwt.t(unit);

let get: (Lwt.t(t), list(string)) => Lwt.t(list((int, Ezjsonm.t)));

let remove: (Lwt.t(t), string, list(list(string))) => Lwt.t(unit);