type t;

let create: (~endpoints: (string, string), ~keys: (string, string)) => t;

let close: (t) => unit;
let send: (t, string) => Lwt.t(unit);
let recv: t => Lwt.t(string);
let route: (t, string, string) => Lwt.t(unit);

let create_ack : int => string;
let create_ack_payload: (int, string) => string;
let create_ack_observe: (string, ~uuid: string) => string;
let create_ack_notification: (string) => string;
let create_header: (~tkl: int, ~oc: int, ~code: int) => (Bitstring.t, int);
let create_option: (~number: int, ~value: string) => (Bitstring.t, int);
let create_content_format: int => string;
let handle_header: Bitstring.t => (int, int, int, Bitstring.t);
let handle_token: (Bitstring.t, int) => (string, Bitstring.t);
let handle_option: ((string, int, int)) => (int, string, Bitstring.t);
let get_observed: array((int, string)) => string;
let get_option_value: (array((int, string)), int) => string;
let get_content_format : array((int, string)) => int;
let get_max_age: array((int, string)) => int32;
let get_uri_path: array((int, string)) => string;
let get_uri_host: array((int, string)) => string;