type t;

let create : (string, string) => (string, string) => t;

/* zmq routines */
let send : t => string => Lwt.t unit;
let recv : t => Lwt.t string;
let route : t => string => string => Lwt.t unit;

/* creating response payloads */
let create_ack : int => string;
let create_ack_payload : int => string => string;
let create_ack_observe : string => uuid::string => string;

/* reading protocol */
let handle_header : Bitstring.t => (int, int, int, Bitstring.t);
let handle_token : Bitstring.t => int => (string, Bitstring.t);
let handle_options : int => Bitstring.t => (array (int, string), Bitstring.t);



