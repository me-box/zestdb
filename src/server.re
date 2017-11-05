open Lwt.Infix;

let rep_endpoint = ref "tcp://0.0.0.0:5555";
let rout_endpoint = ref "tcp://0.0.0.0:5556";
/* let notify_list  = ref [(("",0),[("", Int32.of_int 0)])]; */
let notify_list  = ref [];
let token_secret_key = ref "";
let router_public_key = ref "";
let router_secret_key = ref "";
let log_mode = ref false;
let server_secret_key_file = ref "";
let version = 1;
let identity = ref (Unix.gethostname ());
let content_format = ref "";

/* create stores in local directory by default */
let default_store_directory = "./";
let store_directory = ref default_store_directory;
let kv_json_store = ref (Database.Json.Kv.create file::(!store_directory ^ "/kv-json-store"));
let ts_json_store = ref (Database.Json.Ts.create file::(!store_directory ^ "/ts-json-store"));
let kv_text_store = ref (Database.String.Kv.create file::(!store_directory ^ "/kv-text-store"));
let kv_binary_store = ref (Database.String.Kv.create file::(!store_directory ^ "/kv-binary-store"));

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
  String.trim (of_string msg |> hexdump_s print_chars::false);
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

let observed_paths_exist () => {
  List.length !notify_list > 0;
};

let get_ident path => {
  List.assoc path !notify_list;
};

let time_now () => {
  Int32.of_float (Unix.time ());
};

let add_to_observe uri_path content_format ident max_age => {
  open Int32;
  let key = (uri_path, content_format);
  let expiry = (equal max_age (of_int 0)) ? max_age : add (time_now ()) max_age;
  let value = (ident, expiry);
  if (is_observed key) {
    let _ = Lwt_log_core.info_f "adding ident:%s to existing path:%s with max-age:%lu" ident uri_path max_age;
    let items = get_ident key;
    let new_items = List.cons value items;
    let filtered = List.filter (fun (key',_) => (key' != key)) !notify_list;
    notify_list := List.cons (key, new_items) filtered;
  } else {
    let _ = Lwt_log_core.info_f "adding ident:%s to new path:%s with max-age:%lu" ident uri_path max_age;
    notify_list := List.cons (key, [value]) !notify_list;
  };
};

let handle_header bits => {
  let tuple = [%bitstring
    switch bits {
    | {|code : 8 : unsigned;
        oc : 8 : unsigned;
        tkl : 16 : bigendian;
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
    | {|number : 16 : bigendian; 
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
    {|code : 8 : unsigned;
      oc : 8 : unsigned;
      tkl : 16 : bigendian         
    |}
  ];
  (bits, 32);
};

let create_option number::number value::value => {
  let byte_length = String.length value;
  let bit_length = byte_length * 8;
  let bits = [%bitstring 
    {|number : 16 : bigendian;
      byte_length : 16 : bigendian;
      value : bit_length : string
    |}
  ];
  (bits ,(bit_length+32));
};


let create_options options => {
  let count = Array.length options;
  let values = Array.map (fun (x,y) => x) options;
  let value = Bitstring.concat (Array.to_list values);
  let lengths = Array.map (fun (x,y) => y) options;
  let length = Array.fold_left (fun x y => x + y) 0 lengths;
  (value, length, count);
};

let create_ack code => {
  let (header_value, header_length) = create_header tkl::0 oc::0 code::code;
  let bits = [%bitstring {|header_value : header_length : bitstring|}];
  Bitstring.string_of_bitstring bits;
};

let create_content_format id => {
  let bits = [%bitstring {|id : 16 : bigendian|}];
  Bitstring.string_of_bitstring bits  
};

let create_ack_payload_options format::format => {
  let content_format = create_option number::12 value::format;
  create_options [|content_format|];
};

let create_ack_payload format_code payload => {
  let (options_value, options_length, options_count) = create_ack_payload_options format::(create_content_format format_code);
  let (header_value, header_length) = create_header tkl::0 oc::options_count code::69;  
  let payload_bytes = String.length payload * 8;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      options_value : options_length : bitstring;
      payload : payload_bytes : string
    |}
  ];
  Bitstring.string_of_bitstring bits;
};

let create_ack_observe_options format::format key::key => {
  let content_format = create_option number::12 value::format;
  let public_key = create_option number::2048 value::key;
  create_options [|content_format, public_key|];
};

let create_ack_observe public_key uuid::payload => {
  let (options_value, options_length, options_count) = create_ack_observe_options format::(create_content_format 0) key::public_key;
  let (header_value, header_length) = create_header tkl::0 oc::options_count code::69;  
  let payload_bytes = String.length payload * 8;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      options_value : options_length : bitstring;
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
    | {|id : 16 : bigendian|} => id;
    | {|_|} => failwith "invalid content value";
    };
  ];
  id;
};

let get_max_age options => {
  let value = get_option_value options 14;
  let bits = Bitstring.bitstring_of_string value;
  let seconds = [%bitstring
    switch bits {
    | {|seconds : 32 : bigendian|} => seconds;
    | {|_|} => failwith "invalid max-age value";
    };
  ];
  seconds;
};


let get_key_mode uri_path => {
  let key_path = Str.string_after uri_path 4;
  let key = String.split_on_char '/' key_path |> List.hd;
  let mode = Str.first_chars uri_path 4;
  (key,mode);
};


let publish path payload socket => {
  let msg = Printf.sprintf "%s %s" path payload;
  Lwt_zmq.Socket.send socket msg;
};

let expire l t => {
  open List;
  let f x =>
    switch x {
    | (k,v) => (k, filter (fun (_,t') => (t' > t)) v);
    };
  filter (fun (x,y) => y != []) (map f l);
};

let diff l1 l2 => List.filter (fun x => not (List.mem x l2)) l1;

let list_uuids alist => {
  open List;  
  map (fun (x,y) => hd y) alist;    
};

let route_message alist socket payload => {
  open Lwt_zmq.Socket.Router;  
  let rec loop l => {
    switch l {
      | [] => Lwt.return_unit;
      | [(ident,expiry), ...rest] => {
          send socket (id_of_string ident) [payload] >>=
          /*Lwt_zmq.Socket.send_all socket [ident, payload] >>=*/
            fun _ => Lwt_log_core.debug_f "Routing:\n%s \nto ident:%s with expiry:%lu" (to_hex payload) ident expiry >>=
              fun _ => loop rest;
        };
      };    
  };
  loop alist;
};

let handle_expire socket => {
  if (observed_paths_exist ()) {
    open Lwt_zmq.Socket.Router;
    let new_notify_list = expire !notify_list (time_now ());
    let uuids = diff (list_uuids !notify_list) (list_uuids new_notify_list);
    notify_list := new_notify_list;
    /* send Service Unavailable */
    route_message uuids socket (create_ack 163);
  } else {
    Lwt.return_unit;
  };
};

let route tuple payload socket => {
  let (_,content_format) = tuple;
  route_message (get_ident tuple) socket (create_ack_payload content_format payload);
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

let handle_read_database content_format uri_path => {
  open Common.Ack;
  open Common.Response;
  let (key,mode) = get_key_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/kv/", 50) => Some (Json (Database.Json.Kv.read !kv_json_store key));
  | ("/ts/", 50) => Some (Json (handle_get_read_ts uri_path));
  | ("/kv/", 42) => Some (Binary (Database.String.Kv.read !kv_binary_store key));
  | ("/kv/", 0) => Some (Text (Database.String.Kv.read !kv_text_store key));
  | _ => None;
  };
  switch result {
  | Some content =>
    switch content {
    | Json json => json >>= fun json' => 
        Lwt.return (Payload content_format (Ezjsonm.to_string json'));
    | Text text => text >>= fun text' =>
        Lwt.return (Payload content_format text');
    | Binary binary => binary >>= fun binary' =>
        Lwt.return (Payload content_format binary');
    };
  | None => Lwt.return (Code 128);
  };
};

let handle_read_hypercat () => {
  open Common.Ack;
  Hypercat.get_cat () |> Ezjsonm.to_string |>
    fun s => (Payload 50 s) |> Lwt.return;
};

let handle_get_read content_format uri_path => {
  switch uri_path {
  | "/cat" => handle_read_hypercat ();
  | _ => handle_read_database content_format uri_path; 
  };
};

let to_json payload => {
  open Ezjsonm;
  let parsed = try (Some (from_string payload)) {
  | Parse_error _ => None;
  };
  parsed;
};

let handle_post_write_ts key uri_path payload => {
  let path_list = String.split_on_char '/' uri_path;
  let timestamp = (List.length path_list == 5 && List.nth path_list 3 == "at") ? 
    Some (int_of_string (List.nth path_list 4)) : None;
  let json = to_json payload;
  switch json {
  | Some value => Some (Database.Json.Ts.write !ts_json_store timestamp key value);
  | None => None;
  };  
};

let handle_post_write_kv key uri_path payload => {
  let json = to_json payload;
  switch json {
  | Some value => Some (Database.Json.Kv.write !kv_json_store key value);
  | None => None;
  };  
};

let handle_write_database content_format uri_path payload => {
  open Common.Ack;
  open Ezjsonm;
  let (key,mode) = get_key_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/kv/", 50) => handle_post_write_kv key uri_path payload;
  | ("/ts/", 50) => handle_post_write_ts key uri_path payload;  
  | ("/kv/", 42) => Some (Database.String.Kv.write !kv_binary_store key payload);
  | ("/kv/", 0) => Some (Database.String.Kv.write !kv_text_store key payload);
  | _ => None;
  };
  switch result {
  | Some _ => Lwt.return (Code 65);
  | None => Lwt.return (Code 128);
  };
};

let handle_write_hypercat payload => {
  open Common.Ack;
  let json = to_json payload;
  switch json {
  | Some json => {
      switch (Hypercat.update_cat json) {
        | Ok => (Code 65)
        | Error n => (Code n)
        } |> Lwt.return;
    };
  | None => Lwt.return (Code 128);
  };
};

let handle_post_write content_format uri_path payload => {
  switch uri_path {
  | "/cat" => handle_write_hypercat payload;
  | _ => handle_write_database content_format uri_path payload; 
  };
};

let ack kind => {
  open Common.Ack;
  switch kind {
  | Code n => create_ack n;
  | Payload format data => create_ack_payload format data;
  | Observe key uuid => create_ack_observe key uuid;
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

let handle_content_format options => {
  let content_format = get_content_format options;
  let _ = Lwt_log_core.debug_f "content_format => %d" content_format;
  content_format;
};

let handle_max_age options => {
  let max_age = get_max_age options;
  let _ = Lwt_log_core.debug_f "max_age => %lu" max_age;
  max_age;
};

let handle_get options token => {
  open Common.Ack;
  let content_format = handle_content_format options;
  let uri_path = get_option_value options 11;
  if ((is_valid_token token uri_path "GET") == false) {
    ack (Code 129)
  } else if (has_observed options) {
    let max_age = handle_max_age options;  
    let uuid = create_uuid ();
    add_to_observe uri_path content_format uuid max_age;
    ack (Observe !router_public_key uuid);
  } else {
    handle_get_read content_format uri_path >>= ack;
  };
};


let handle_post options token payload with::rout_soc => {
  open Common.Ack;
  let content_format = handle_content_format options;
  let uri_path = get_option_value options 11;
  let tuple = (uri_path, content_format);
  if ((is_valid_token token uri_path "POST") == false) {
    ack (Code 129);
  } else if (is_observed tuple) {
      handle_post_write content_format uri_path payload >>=
        fun resp => {
          /* we dont want to route bad requests */
          if (resp != (Code 128)) {
            route tuple payload rout_soc >>= fun () => ack resp;
          } else {
            ack resp;
          };
      };
  } else {
    handle_post_write content_format uri_path payload >>= ack;
  };
};

let handle_msg msg with::rout_soc => {
  handle_expire rout_soc >>=
    fun () =>
      Lwt_log_core.debug_f "Received:\n%s" (to_hex msg) >>=
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
                Lwt_log_core.debug_f "Sending:\n%s" (to_hex resp) >>=
                  fun () => loop ();
  };
  loop ();
};

let setup_rep_socket endpoint ctx kind secret => {
  open ZMQ.Socket;
  let soc = ZMQ.Socket.create ctx kind;
  set_linger_period soc 0;
  set_curve_server soc true;
  set_curve_secretkey soc secret; 
  bind soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let setup_rout_socket endpoint ctx kind secret => {
  open ZMQ.Socket;
  let soc = ZMQ.Socket.create ctx kind;
  /* ZMQ.Socket.set_receive_high_water_mark soc 1; */
  /* ZMQ.Socket.set_send_high_water_mark soc 1; */
  set_linger_period soc 0;
  set_curve_server soc true;
  set_curve_secretkey soc secret; 
  bind soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let close_socket lwt_soc => {
  let soc = Lwt_zmq.Socket.to_socket lwt_soc;
  ZMQ.Socket.close soc;
};


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
    ("--secret-key-file", Arg.Set_string server_secret_key_file, ": to set the curve secret key"),
    ("--token-key", Arg.Set_string token_secret_key, ": to set the token secret key"),
    ("--identity", Arg.Set_string identity, ": to set the server identity"),
    ("--store-dir", Arg.Set_string store_directory, ": to set the location for the database files"),
  ];
  Arg.parse speclist (fun x => raise (Arg.Bad ("Bad argument : " ^ x))) usage;
};

let setup_router_keys () => {
  let (public_key,private_key) = ZMQ.Curve.keypair ();
  router_secret_key := private_key;
  router_public_key := public_key;
};

/* some issues running these threads so disabled */
let monitor_connections ctx rep_soc rout_soc => {
  let () = Connections.monitor ctx rout_soc;
  let () = Connections.monitor ctx rep_soc;
};

/* support overriding location of stores */
let create_stores_again () => {
  kv_json_store := Database.Json.Kv.create file::(!store_directory ^ "/kv-json-store");
  ts_json_store := Database.Json.Ts.create file::(!store_directory ^ "/ts-json-store");
  kv_text_store := Database.String.Kv.create file::(!store_directory ^ "/kv-text-store");
  kv_binary_store := Database.String.Kv.create file::(!store_directory ^ "/kv-binary-store");
};

let get_key_from_file file => {
  Fpath.v file |>
    Bos.OS.File.read |>
      fun result =>
        switch result {
        | Rresult.Error _ => failwith "failed to get key from file";
        | Rresult.Ok key => key;
        };
};

let rec run_server () => {
  parse_cmdline ();
  !log_mode ? setup_logger () : ();
  setup_router_keys ();
  (!store_directory != default_store_directory) ? create_stores_again () : ();
  let ctx = ZMQ.Context.create ();
  let rep_soc = setup_rep_socket !rep_endpoint ctx ZMQ.Socket.rep (get_key_from_file !server_secret_key_file);
  let rout_soc = setup_rout_socket !rout_endpoint ctx ZMQ.Socket.router !router_secret_key;
  let _ = Lwt_log_core.info "Ready";   
  let _ = try (Lwt_main.run {server with::rep_soc and::rout_soc}) {
    | e => report_error e;
  };
  close_socket rout_soc;
  close_socket rep_soc;
  ZMQ.Context.terminate ctx;
  run_server ();
};

/* setup_keys (); */
run_server ();

