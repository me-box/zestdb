type t;

let create : endpoints::(string, string) => keys::(string, string) => t;
let close : t => unit;

/* zmq routines */
let send : t => string => Lwt.t unit;
let recv : t => Lwt.t string;
let route : t => string => string => Lwt.t unit;

/* creating response payloads */
let create_ack : int => string;
let create_ack_payload : int => string => string;
let create_ack_observe : string => uuid::string => string;

/* writing protocol */
let create_header : tkl::int => oc::int => code::int => (Bitstring.t, int);
let create_option : number::int => value::string => (Bitstring.t, int);
let create_content_format : int => string;

/* reading protocol */
let handle_header : Bitstring.t => (int, int, int, Bitstring.t);
let handle_token : Bitstring.t => int => (string, Bitstring.t);
let handle_options : int => Bitstring.t => (array (int, string), Bitstring.t);

/* read option values */
let get_option_value : array (int, string) => int => string;
let get_content_format : array (int, string) => int;
let get_max_age : array (int, string) => int32;
