open Lwt.Infix;

let rep_endpoint = ref "tcp://0.0.0.0:5555";
let rout_endpoint = ref "tcp://0.0.0.0:5556";
let notify_list  = ref [("",[""])];
let token_secret_key = ref "";
let version = 1;
let identity = ref (Unix.gethostname ());
let content_format = ref "2"; /* ascii equivalent of 50 representing json */

let kv_json_store = ref (Database.Json.Kv.create file::"./kv-json-store");
let ts_json_store = ref (Database.Json.Ts.create file::"./ts-json-store");

let setup_logger () => {
  Lwt_log_core.default :=
    Lwt_log.channel
      template::"$(date).$(milliseconds) [$(level)] $(message)"
      close_mode::`Keep
      channel::Lwt_io.stdout
      ();
  Lwt_log_core.add_rule "*" Lwt_log_core.Error;
  Lwt_log_core.add_rule "*" Lwt_log_core.Info;
  Lwt_log_core.add_rule "*" Lwt_log_core.Debug;
};

let to_hex msg => {
  open Hex;
  String.trim (of_string msg |> hexdump_s);
};

let has_observed options => {
  if (Array.exists (fun (number,_) => number == 6) options) {
    true;
  } else {
    false;
  }
};

let is_observed path => {
  List.mem_assoc path !notify_list;
};

let get_ident path => {
  List.assoc path !notify_list;
};

let add_to_observe path ident => {
  if (is_observed path) {
    let _ = Lwt_log_core.info_f "adding ident:%s to existing path:%s" ident path;
    let items = get_ident path;
    let new_items = List.cons ident items;
    let filtered = List.filter (fun (path',_) => (path' != path)) !notify_list;
    notify_list := List.cons (path, new_items) filtered;
  } else {
    let _ = Lwt_log_core.info_f "adding ident:%s to new path:%s" ident path;
    notify_list := List.cons (path, [ident]) !notify_list;
  };
};


let publish path payload socket => {
  let msg = Printf.sprintf "%s %s" path payload;
  Lwt_zmq.Socket.send socket msg;
};


let route path payload socket => {
  open Lwt_zmq.Socket.Router;
  let rec loop l => {
    switch l {
    | [] => Lwt.return_unit;
    | [ident, ...rest] => {
        send socket (id_of_string ident) [payload] >>=
        /*Lwt_zmq.Socket.send_all socket [ident, payload] >>=*/
          fun _ => Lwt_log_core.debug_f "sending payload:%s to ident:%s ident" payload ident >>=
            fun _ => loop rest;
      };
    };
  };
  loop (get_ident path);
};

let handle_header bits => {
  let tuple = [%bitstring
    switch bits {
    | {|version : 4 : unsigned;
        oc : 4 : unsigned;
        tkl : 16 : bigendian; 
        code : 8 : unsigned; 
        rest : -1 : bitstring
     |} => (tkl, oc, code, rest); 
    | {|_|} => failwith "invalid header";
    };
  ];
  tuple;    
};

let handle_token bits len => {
  let tuple = [%bitstring
    switch bits {
    | {|token : len*8 : string; 
        rest : -1 : bitstring
      |} => (token, rest);
    | {|_|} => failwith "invalid token";
    };
  ];
  tuple;
};

let handle_option bits => {
  let tuple = [%bitstring
    switch bits {
    | {|number : 8 : unsigned; 
        len : 16 : bigendian;
        value: len*8: string; 
        rest : -1 : bitstring
      |} => (number, value, rest);
    | {|_|} => failwith "invalid options";
    };
  ];
  tuple;
};

let handle_options oc bits => {
  let options = Array.make oc (0,"");
  let rec handle oc bits =>
    if (oc == 0) {
      bits;
    } else {
      let (number, value, r) = handle_option bits;
      Array.set options (oc - 1) (number,value);
      let _ = Lwt_log_core.debug_f "option => %d:%s" number value;
      handle (oc - 1) r
  };
  (options, handle oc bits);
};

let create_header tkl::tkl oc::oc code::code => {
  let bits = [%bitstring 
    {|version : 4: unsigned;
      tkl : 16 : bigendian;
      oc : 4 : unsigned;
      code : 8 : unsigned
    |}
  ];
  (bits, 32);
};

let create_option number::number value::value => {
  let byte_length = String.length value;
  let bit_length = byte_length * 8;
  let bits = [%bitstring 
    {|number : 8 : unsigned;
      byte_length : 16 : bigendian;
      value : bit_length : string
    |}
  ];
  (bits ,(bit_length+24));
};

let create_ack code => {
  let (header_value, header_length) = create_header tkl::0 oc::0 code::code;
  let bits = [%bitstring {|header_value : header_length : bitstring|}];
  Bitstring.string_of_bitstring bits;
};

let create_ack_payload payload => {
  let (header_value, header_length) = create_header tkl::0 oc::1 code::69;
  let (format_value, format_length) = create_option number::12 value::!content_format;
  let payload_bytes = String.length payload * 8;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      format_value : format_length : bitstring;
      payload : payload_bytes : string
    |}
  ];
  Bitstring.string_of_bitstring bits;
};

let get_option_value options value => {
  let rec find a x i => {
    let (number,value) = a.(i);
    if (number == x) {
      value;
    } else {
      find a x (i + 1)
    };
  };
  find options value 0;
};

let get_content_format options => {
  let value = get_option_value options 12;
  let bits = Bitstring.bitstring_of_string value;
  let id = [%bitstring
    switch bits {
    | {|id : 8 : unsigned|} => id;
    | {|_|} => failwith "invalid content value";
    };
  ];
  id;
};


let get_key_mode uri_path => {
  let key = Str.string_after uri_path 4;
  let mode = Str.first_chars uri_path 4;
  (key,mode);
};

let handle_get_read_ts_latest path_list => {
  let id = List.nth path_list 2;
  Database.Json.Ts.read_latest !ts_json_store id;
};

let handle_get_read_ts_last path_list => {
  let id = List.nth path_list 2;
  let n = List.nth path_list 4;
  Database.Json.Ts.read_last !ts_json_store id (int_of_string n);
};

let handle_get_read_ts_since path_list => {
  let id = List.nth path_list 2;
  let t = List.nth path_list 4;
  Database.Json.Ts.read_since !ts_json_store id (int_of_string t);
};

let handle_get_read_ts_range path_list => {
  let id = List.nth path_list 2;
  let t1 = List.nth path_list 4;
  let t2 = List.nth path_list 5;
  Database.Json.Ts.read_range !ts_json_store id (int_of_string t1) (int_of_string t2);
};

let handle_get_read_ts uri_path => {
  let path_list = String.split_on_char '/' uri_path;
  let mode = List.nth path_list 3;
  switch mode {
  | "latest" => handle_get_read_ts_latest path_list;
  | "last" => handle_get_read_ts_last path_list;
  | "since" => handle_get_read_ts_since path_list;
  | "range" => handle_get_read_ts_range path_list;
  | _ => failwith ("unsupported get ts mode:" ^ mode);
  };
};

let handle_read_database uri_path => {
  open Common.Ack;
  let (key,mode) = get_key_mode uri_path;
  switch mode {
  | "/kv/" => Database.Json.Kv.read !kv_json_store key;
  | "/ts/" => handle_get_read_ts uri_path;
  | _ => failwith "unsupported get mode";
  } >>= fun json => Lwt.return (Payload (Ezjsonm.to_string json));
};

let handle_read_hypercat () => {
  open Common.Ack;
  Hypercat.get_cat () |> Ezjsonm.to_string |>
    fun s => (Payload s) |> Lwt.return;
};

let handle_get_read uri_path => {
  switch uri_path {
  | "/cat" => handle_read_hypercat ();
  | _ => handle_read_database uri_path; 
  };
};

let handle_write_database uri_path json => {
  open Common.Ack;
  let (key,mode) = get_key_mode uri_path;
  switch mode {
  | "/kv/" => Database.Json.Kv.write !kv_json_store key json;
  | "/ts/" => Database.Json.Ts.write !ts_json_store key json;
  | _ => failwith "unsupported post mode";
  } >>= fun () => Lwt.return (Code 65);
};

let handle_write_hypercat json => {
  open Common.Ack;
  switch (Hypercat.update_cat json) {
  | Ok => (Code 65)
  | Error n => (Code n)
  } |> Lwt.return;
};

let handle_post_write uri_path payload => {
  open Common.Ack;
  open Ezjsonm;
  let parsed = try (Some (from_string payload)) {
  | Parse_error _ => None;
  };
  switch parsed {
  | None => Lwt.return (Code 143);
  | Some json => 
      switch uri_path {
      | "/cat" => handle_write_hypercat json;
      | _ => handle_write_database uri_path json; 
      };
  };
};

let ack kind => {
  open Common.Ack;
  switch kind {
  | Code n => create_ack n;
  | Payload s => create_ack_payload s;
  } |> Lwt.return;
};

let create_uuid () => {
  Uuidm.v4_gen (Random.State.make_self_init ()) () |> Uuidm.to_string;
};

let is_valid_token token path meth => {
  switch !token_secret_key {
  | "" => true;
  | _ => Token.is_valid token !token_secret_key ["path = " ^ path, "method = " ^ meth, "target = " ^ !identity];
  };
};

let handle_get options token => {
  open Common.Ack;
  let uri_path = get_option_value options 11;
  if ((is_valid_token token uri_path "GET") == false) {
    ack (Code 129)
  } else if (has_observed options) {
    let uuid = create_uuid ();
    add_to_observe uri_path uuid;
    ack (Payload uuid);
  } else {
    handle_get_read uri_path >>= ack;
  };
};

let assert_content_format options => {
  let content_format = get_content_format options;
  let _ = Lwt_log_core.debug_f "content_format => %d" content_format;
  assert (content_format == 50);
};

let handle_post options token payload with::rout_soc => {
  open Common.Ack;
  /* we are just accepting json for now */
  assert_content_format options;
  let uri_path = get_option_value options 11;
  if ((is_valid_token token uri_path "POST") == false) {
    ack (Code 129);
  } else if (is_observed uri_path) {
    route uri_path payload rout_soc >>=
      fun () => handle_post_write uri_path payload >>= ack;
  } else {
    handle_post_write uri_path payload >>= ack;
  };
};

let handle_msg msg with::rout_soc => {
  Lwt_log_core.debug_f "Received:%s\n%s" msg (to_hex msg) >>=
    fun () => {
      let r0 = Bitstring.bitstring_of_string msg;
      let (tkl, oc, code, r1) = handle_header r0;
      let (token, r2) = handle_token r1 tkl;
      let (options,r3) = handle_options oc r2;
      let payload = Bitstring.string_of_bitstring r3;
      switch code {
      | 1 => handle_get options token;
      | 2 => handle_post options token payload with::rout_soc;
      | _ => failwith "invalid code";
      };
    };  
};

let server with::rep_soc and::rout_soc => {
  let rec loop () => {
    Lwt_zmq.Socket.recv rep_soc >>=
      fun msg =>
        handle_msg msg with::rout_soc >>=
          fun resp =>
            Lwt_zmq.Socket.send rep_soc resp >>=
              fun () =>
                Lwt_log_core.debug_f "Sending:%s\n%s" resp (to_hex resp) >>=
                  fun () => loop ();
  };
  loop ();
};

let connect_socket endpoint ctx kind secret => {
  let soc = ZMQ.Socket.create ctx kind;
  ZMQ.Socket.set_linger_period soc 0;
  ZMQ.Socket.set_curve_server soc true;
  ZMQ.Socket.set_curve_secretkey soc secret; 
  ZMQ.Socket.bind soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let close_socket lwt_soc => {
  let soc = Lwt_zmq.Socket.to_socket lwt_soc;
  ZMQ.Socket.close soc;
};

let log_mode = ref false;
let curve_secret_key = ref "";

/* test key: uf4XGHI7[fLoe&aG1tU83[ptpezyQMVIHh)J=zB1 */

let report_error e => {
  let msg = Printexc.to_string e;
  let stack = Printexc.get_backtrace ();
  let _ = Lwt_log_core.error_f "Opps: %s%s" msg stack;
};

let parse_cmdline () => {
  let usage = "usage: " ^ Sys.argv.(0) ^ " [--debug] [--secret-key string]";
  let speclist = [
    ("--request-endpoint", Arg.Set_string rep_endpoint, ": to set the request/reply endpoint"),
    ("--router-endpoint", Arg.Set_string rout_endpoint, ": to set the router/dealer endpoint"),
    ("--enable-logging", Arg.Set log_mode, ": turn debug mode on"),
    ("--secret-key", Arg.Set_string curve_secret_key, ": to set the curve secret key"),
    ("--token-key", Arg.Set_string token_secret_key, ": to set the token secret key"),
    ("--identity", Arg.Set_string identity, ": to set the server identity"),
  ];
  Arg.parse speclist (fun x => raise (Arg.Bad ("Bad argument : " ^ x))) usage;
};

/* experimental idea to add key to hypercat and have unencrypted read access */
let setup_keys () => {
  let (public_key,private_key) = ZMQ.Curve.keypair ();
  curve_secret_key := private_key;
  let base_item = Ezjsonm.from_channel (open_in "base-item.json");
  Hypercat.update_item base_item "urn:X-hypercat:rels:publicKey" public_key;
};

let rec run_server () => {
  parse_cmdline ();
  !log_mode ? setup_logger () : ();
  let ctx = ZMQ.Context.create ();
  let rep_soc = connect_socket !rep_endpoint ctx ZMQ.Socket.rep !curve_secret_key;
  let rout_soc = connect_socket !rout_endpoint ctx ZMQ.Socket.router !curve_secret_key;
  let _ = Lwt_log_core.info "Ready";
  let _ = try (Lwt_main.run { server with::rep_soc and::rout_soc}) {
    | e => report_error e;
  };
  close_socket rout_soc;
  close_socket rep_soc;
  ZMQ.Context.terminate ctx;
  run_server ();
};

/* setup_keys (); */
run_server ();

