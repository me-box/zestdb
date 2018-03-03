open Lwt.Infix;

let rep_endpoint = ref "tcp://0.0.0.0:5555";
let rout_endpoint = ref "tcp://0.0.0.0:5556";
/* let notify_list  = ref [(("",0),[("", Int32.of_int 0)])]; */
let token_secret_key_file = ref "";
let token_secret_key = ref "";
let router_public_key = ref "";
let router_secret_key = ref "";
let log_mode = ref false;
let server_secret_key_file = ref "";
let server_secret_key = ref "";
let version = 1;
let identity = ref (Unix.gethostname ());
let content_format = ref "";

/* create stores in local directory by default */
let store_directory = ref "./";

module Ack = {
  type t = Code int |  Payload int string | Observe string string;
};

module Response = {
  type t = Empty | Json (Lwt.t Ezjsonm.t) | Text (Lwt.t string) | Binary (Lwt.t string);
};

type t = {
  observe_ctx: Observe.t,
  numts_ctx: Numeric_timeseries.t,
  blobts_ctx: Blob_timeseries.t,
  jsonkv_ctx: Keyvalue.Json.t,
  textkv_ctx: Keyvalue.Text.t,
  binarykv_ctx: Keyvalue.Binary.t,
  zmq_ctx: Protocol.Zest.t, 
  version: int
};


let time_now () => {
  Int32.of_float (Unix.time ());
};



let route_message alist ctx payload => {
  open Logger;
  let rec loop l => {
    switch l {
      | [] => Lwt.return_unit;
      | [(ident,expiry,mode), ...rest] => {
          Protocol.Zest.route ctx.zmq_ctx ident payload >>= fun () =>
            debug_f "route_message" (Printf.sprintf "Routing:\n%s \nto ident:%s with expiry:%lu" (to_hex payload) ident expiry) >>= 
            fun () => loop rest;
        };
      };    
  };
  loop alist;
};

let route tuple payload ctx => {
  let (_,content_format) = tuple;
  route_message (Observe.get ctx.observe_ctx tuple) ctx (Protocol.Zest.create_ack_payload content_format payload);
};

let handle_expire ctx => {
  Observe.expire ctx.observe_ctx >>=
    fun uuids => route_message uuids ctx (Protocol.Zest.create_ack 163);
};


let handle_get_read_ts_numeric_latest id ctx => {
  open Response;  
  Json (Numeric_timeseries.read_latest ctx::ctx.numts_ctx id::id fn::[]);
};

let handle_get_read_ts_blob_latest id ctx => {
  open Response;
  Json (Blob_timeseries.read_latest ctx::ctx.blobts_ctx id::id);
};


let handle_get_read_ts_numeric_earliest id ctx => {
  open Response;  
  Json (Numeric_timeseries.read_earliest ctx::ctx.numts_ctx id::id fn::[]);
};

let handle_get_read_ts_blob_earliest id ctx => {
  open Response;
  Json (Blob_timeseries.read_earliest ctx::ctx.blobts_ctx id::id);
};

let apply path apply0 apply1 apply2 => {
  open Response;
  open Numeric;
  open Filter;
  switch path {
    | [] => apply0;
    | ["sum"] => apply1 sum;
    | ["count"] => apply1 count;
    | ["min"] => apply1 min;
    | ["max"] => apply1 max;
    | ["mean"] => apply1 mean;
    | ["median"] => apply1 median;
    | ["sd"] => apply1 sd;
    | ["filter", s1, "equals", s2] => apply1 (equals s1 s2);
    | ["filter", s1, "contains", s2] => apply1 (contains s1 s2);
    | ["filter", s1, "equals", s2, "sum"] => apply2 (equals s1 s2) sum;
    | ["filter", s1, "contains", s2, "sum"] => apply2 (contains s1 s2) sum;
    | ["filter", s1, "equals", s2, "count"] => apply2 (equals s1 s2) count;
    | ["filter", s1, "contains", s2, "count"] => apply2 (contains s1 s2) count;
    | ["filter", s1, "equals", s2, "min"] => apply2 (equals s1 s2) min;
    | ["filter", s1, "contains", s2, "min"] => apply2 (contains s1 s2) min;
    | ["filter", s1, "equals", s2, "max"] => apply2 (equals s1 s2) max;
    | ["filter", s1, "contains", s2, "max"] => apply2 (contains s1 s2) max;
    | ["filter", s1, "equals", s2, "mean"] => apply2 (equals s1 s2) mean;
    | ["filter", s1, "contains", s2, "mean"] => apply2 (contains s1 s2) mean;
    | ["filter", s1, "equals", s2, "median"] => apply2 (equals s1 s2) median;
    | ["filter", s1, "contains", s2, "median"] => apply2 (contains s1 s2) median;
    | ["filter", s1, "equals", s2, "sd"] => apply2 (equals s1 s2) sd;
    | ["filter", s1, "contains", s2, "sd"] => apply2 (contains s1 s2) sd;
    | _ => Empty;
    };  
};

let handle_get_read_ts_numeric_last id n func ctx => {
  open Response;
  open Numeric_timeseries;
  let apply0 = Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
  let apply1 f => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
  let apply2 f1 f2 => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
  apply func apply0 apply1 apply2;
};

let handle_get_read_ts_blob_last id n ctx => {
  open Response;  
  Json (Blob_timeseries.read_last ctx::ctx.blobts_ctx id::id n::(int_of_string n));
};

let handle_get_read_ts_numeric_first id n func ctx => {
  open Response;
  open Numeric_timeseries;
  let apply0 = Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
  let apply1 f => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
  let apply2 f1 f2 => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
  apply func apply0 apply1 apply2;    
};

let handle_get_read_ts_blob_first id n ctx => {
  open Response;  
  Json (Blob_timeseries.read_first ctx::ctx.blobts_ctx id::id n::(int_of_string n));
};

let handle_get_read_ts_numeric_since id t func ctx => {
  open Response;
  open Numeric_timeseries;
  let apply0 = Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[]);
  let apply1 f => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f]);
  let apply2 f1 f2 => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f1, f2]);   
  apply func apply0 apply1 apply2;
};

let handle_get_read_ts_blob_since id t ctx => {
  open Response;  
  Json (Blob_timeseries.read_since ctx::ctx.blobts_ctx id::id from::(int_of_string t));
};


let handle_get_read_ts_numeric_range id t1 t2 func ctx => {
  open Response;  
  open Numeric_timeseries;
  let apply0 = Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[]);
  let apply1 f => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f]);
  let apply2 f1 f2 => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f1, f2]);
  apply func apply0 apply1 apply2;
};

let handle_get_read_ts_blob_range id t1 t2 ctx => {
  open Response;  
  Json (Blob_timeseries.read_range ctx::ctx.blobts_ctx id::id from::(int_of_string t1) to::(int_of_string t2));
};

let handle_get_read_ts_numeric_length id ctx => {
  open Response;
  Json (Numeric_timeseries.length ctx::ctx.numts_ctx id::id);
};

let handle_get_read_ts_blob_length id ctx => {
  open Response;
  Json (Blob_timeseries.length ctx::ctx.blobts_ctx id::id);
};

let handle_get_read_ts uri_path ctx => {
  open List;
  open Response;  
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", "blob", id, "length"] => handle_get_read_ts_blob_length id ctx;
  | ["", "ts", id, "length"] => handle_get_read_ts_numeric_length id ctx;
  | ["", "ts", "blob", id, "latest"] => handle_get_read_ts_blob_latest id ctx;
  | ["", "ts", id, "latest"] => handle_get_read_ts_numeric_latest id ctx;
  | ["", "ts", "blob", id, "earliest"] => handle_get_read_ts_blob_earliest id ctx;
  | ["", "ts", id, "earliest"] => handle_get_read_ts_numeric_earliest id ctx;
  | ["", "ts", "blob", id, "last", n] => handle_get_read_ts_blob_last id n ctx;
  | ["", "ts", id, "last", n, ...func] => handle_get_read_ts_numeric_last id n func ctx;
  | ["", "ts", "blob", id, "first", n] => handle_get_read_ts_blob_first id n ctx;
  | ["", "ts", id, "first", n, ...func] => handle_get_read_ts_numeric_first id n func ctx;
  | ["", "ts", "blob", id, "since", t] => handle_get_read_ts_blob_since id t ctx;
  | ["", "ts", id, "since", t, ...func] => handle_get_read_ts_numeric_since id t func ctx;
  | ["", "ts", "blob", id, "range", t1, t2] => handle_get_read_ts_blob_range id t1 t2 ctx;
  | ["", "ts", id, "range", t1, t2, ...func] => handle_get_read_ts_numeric_range id t1 t2 func ctx;
  | _ => Empty;
  };
};


let get_id_key mode uri_path => {
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", mode, id, key] => Some (id, key);
  | _ => None;
  };
};

let get_mode uri_path => {
  Str.first_chars uri_path 4;
};


let handle_get_read_kv_json uri_path ctx => {
  open Response;
  open Keyvalue.Json;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Json (read branch::ctx.jsonkv_ctx id::id key::key);
  | None => Empty;
  };
};

let handle_get_read_kv_text uri_path ctx => {
  open Response;
  open Keyvalue.Text;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Text (read branch::ctx.textkv_ctx id::id key::key);
  | None => Empty;
  };
};

let handle_get_read_kv_binary uri_path ctx => {
  open Response;
  open Keyvalue.Binary;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Text (read branch::ctx.binarykv_ctx id::id key::key);
  | None => Empty;
  };
};

let handle_read_database content_format uri_path ctx => {
  open Ack;
  open Response;
  let mode = get_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/ts/", 50) => handle_get_read_ts uri_path ctx;
  | ("/kv/", 50) => handle_get_read_kv_json uri_path ctx;
  | ("/kv/", 0) => handle_get_read_kv_text uri_path ctx;
  | ("/kv/", 42) => handle_get_read_kv_binary uri_path ctx;
  | _ => Empty;
  };
  switch result {
  | Json json => json >>= fun json' => 
        Lwt.return (Payload content_format (Ezjsonm.to_string json'));
  | Text text => text >>= fun text' =>
        Lwt.return (Payload content_format text');
  | Binary binary => binary >>= fun binary' =>
        Lwt.return (Payload content_format binary');
  | Empty => Lwt.return (Code 128);
  };
};

let handle_read_hypercat () => {
  open Ack;
  Hypercat.get_cat () |> Ezjsonm.to_string |>
    fun s => (Payload 50 s) |> Lwt.return;
};

let handle_get_read content_format uri_path ctx => {
  switch uri_path {
  | "/cat" => handle_read_hypercat ();
  | _ => handle_read_database content_format uri_path ctx; 
  };
};

let to_json payload => {
  open Ezjsonm;
  let parsed = try (Some (from_string payload)) {
  | Parse_error _ => None;
  };
  parsed;
};




let handle_post_write_ts_numeric ::timestamp=None key payload ctx => {
  open Numeric_timeseries;
  let json = to_json payload;
  switch json {
  | Some value => {
      if (is_valid value) {
        Some (write ctx::ctx.numts_ctx timestamp::timestamp id::key json::value);
      } else None;
    };
  | None => None;
  };  
};

let handle_post_write_ts_blob ::timestamp=None key payload ctx => {
  open Blob_timeseries;
  let json = to_json payload;
  switch json {
  | Some value => Some (write ctx::ctx.blobts_ctx timestamp::timestamp id::key json::value);
  | None => None;
  };  
};

let handle_post_write_ts uri_path payload ctx => {
  open List;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", "blob", key] =>
    handle_post_write_ts_blob key payload ctx;
  | ["", "ts", "blob", key, "at", ts] => 
    handle_post_write_ts_blob timestamp::(Some (int_of_string ts)) key payload ctx;
  | ["", "ts", key] => 
    handle_post_write_ts_numeric key payload ctx;
  | ["", "ts", key, "at", ts] => 
    handle_post_write_ts_numeric timestamp::(Some (int_of_string ts)) key payload ctx;
  | _ => None;
  };
};

let handle_post_write_kv_json uri_path payload ctx => {
  open Keyvalue.Json;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => switch (to_json payload) {
    | Some json => Some (write branch::ctx.jsonkv_ctx id::id key::key json::json);
    | None => None;
    }
  | None => None;
  };
};

let handle_post_write_kv_text uri_path payload ctx => {
  open Keyvalue.Text;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Some (write branch::ctx.textkv_ctx id::id key::key text::payload);
  | None => None;
  };
};

let handle_post_write_kv_binary uri_path payload ctx => {
  open Keyvalue.Binary;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Some (write branch::ctx.binarykv_ctx id::id key::key binary::payload);
  | None => None;
  };
};

let handle_write_database content_format uri_path payload ctx => {
  open Ack;
  open Ezjsonm;
  let mode = get_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/ts/", 50) => handle_post_write_ts uri_path payload ctx; 
  | ("/kv/", 50) => handle_post_write_kv_json uri_path payload ctx;
  | ("/kv/", 0) => handle_post_write_kv_text uri_path payload ctx;
  | ("/kv/", 42) => handle_post_write_kv_binary uri_path payload ctx;  
  | _ => None;
  };
  switch result {
  | Some promise => promise >>= fun () => Lwt.return (Code 65);
  | None => Lwt.return (Code 128);
  };
};

let handle_write_hypercat payload => {
  open Ack;
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

let handle_post_write content_format uri_path payload ctx => {
  switch uri_path {
  | "/cat" => handle_write_hypercat payload;
  | _ => handle_write_database content_format uri_path payload ctx; 
  };
};

let ack kind => {
  open Ack;
  switch kind {
  | Code n => Protocol.Zest.create_ack n;
  | Payload format data => Protocol.Zest.create_ack_payload format data;
  | Observe key uuid => Protocol.Zest.create_ack_observe key uuid;
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

let handle_options oc bits => {
  let options = Array.make oc (0,"");
  let rec handle oc bits =>
    if (oc == 0) {
      bits;
    } else {
      let (number, value, r) = Protocol.Zest.handle_option bits;
      let _ = Logger.debug_f "handle_options" (Printf.sprintf "%d:%s" number value);     
      Array.set options (oc - 1) (number,value);
      handle (oc - 1) r
  };
  (options, handle oc bits);
};

let handle_content_format options => {
  let content_format = Protocol.Zest.get_content_format options;
  Logger.debug_f "handle_content_format" (Printf.sprintf "%d" content_format) >>= 
    fun () => Lwt.return content_format;
};

let handle_max_age options => {
  let max_age = Protocol.Zest.get_max_age options;
  Logger.debug_f "handle_max_age" (Printf.sprintf "max_age => %lu" max_age) >>=
    fun () => Lwt.return max_age;
};


let handle_get options token ctx => {
  handle_content_format options >>= fun content_format => {
    let uri_path = Protocol.Zest.get_option_value options 11;
    let observe_mode = Protocol.Zest.observed options;
    if ((is_valid_token token uri_path "GET") == false) {
      ack (Ack.Code 129)
    } else if ((observe_mode == "data") || (observe_mode == "audit")) {
      handle_max_age options >>= fun max_age => {
        let uuid = create_uuid ();
        Observe.add ctx.observe_ctx uri_path content_format uuid max_age observe_mode >>=
          fun x => ack (Ack.Observe !router_public_key uuid);
      };
    } else {
      handle_get_read content_format uri_path ctx >>= ack;
    };
  };
};

let handle_post options token payload ctx => {
  open Ack;
  handle_content_format options >>= fun content_format => {
    let uri_path = Protocol.Zest.get_option_value options 11;
    let key = (uri_path, content_format);
    if ((is_valid_token token uri_path "POST") == false) {
      ack (Code 129); 
    } else if (Observe.is_observed ctx.observe_ctx key) {
        handle_post_write content_format uri_path payload ctx >>=
          fun resp => {
            /* we dont want to route bad requests */
            if (resp != (Code 128)) {
              route key payload ctx >>= fun () => ack resp;
            } else {
              ack resp;
            };
        };
    } else {
      handle_post_write content_format uri_path payload ctx >>= ack;
    };
  };
};

let handle_msg msg ctx => {
  open Logger;
  handle_expire ctx >>=
    fun () =>
      Logger.debug_f "handle_msg" (Printf.sprintf "Received:\n%s" (to_hex msg)) >>=
        fun () => {
          let r0 = Bitstring.bitstring_of_string msg;
          let (tkl, oc, code, r1) = Protocol.Zest.handle_header r0;
          let (token, r2) = Protocol.Zest.handle_token r1 tkl;
          let (options,r3) = handle_options oc r2;
          let payload = Bitstring.string_of_bitstring r3;
          switch code {
          | 1 => handle_get options token ctx;
          | 2 => handle_post options token payload ctx;
          | _ => failwith "invalid code";
          };
        };  
};

let server ctx => {
  open Logger;
  let rec loop () => {
    Protocol.Zest.recv ctx.zmq_ctx >>=
      fun msg => handle_msg msg ctx >>=
        fun resp => Protocol.Zest.send ctx.zmq_ctx resp >>=
          fun () => Logger.debug_f "server" (Printf.sprintf "Sending:\n%s" (to_hex resp)) >>=
            fun () => loop ();
  };
  Logger.info_f "server" "active" >>= fun () => loop ();
};



/* test key: uf4XGHI7[fLoe&aG1tU83[ptpezyQMVIHh)J=zB1 */


let parse_cmdline () => {
  let usage = "usage: " ^ Sys.argv.(0) ^ " [--debug] [--secret-key string]";
  let speclist = [
    ("--request-endpoint", Arg.Set_string rep_endpoint, ": to set the request/reply endpoint"),
    ("--router-endpoint", Arg.Set_string rout_endpoint, ": to set the router/dealer endpoint"),
    ("--enable-logging", Arg.Set log_mode, ": turn debug mode on"),
    ("--secret-key-file", Arg.Set_string server_secret_key_file, ": to set the curve secret key"),
    ("--token-key-file", Arg.Set_string token_secret_key_file, ": to set the token secret key"),
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

let data_from_file file => {
  Fpath.v file |>
    Bos.OS.File.read |>
      fun result =>
        switch result {
        | Rresult.Error _ => failwith "failed to access file";
        | Rresult.Ok key => key;
        };
};

let set_server_key file => {
  server_secret_key := (data_from_file file);
};

let set_token_key file => {
  if (file != "") { 
    token_secret_key := (data_from_file file);
  };
};

let cleanup_router ctx => {
  Observe.get_all ctx.observe_ctx |>
    fun uuids => route_message uuids ctx (Protocol.Zest.create_ack 163) >>=
      fun () => Lwt_unix.sleep 1.0;
};

let terminate_server ctx => {
  Lwt_io.printf "\nShutting down server...\n" >>= fun () =>
    Blob_timeseries.flush ctx::ctx.blobts_ctx >>= fun () =>
      Numeric_timeseries.flush ctx::ctx.numts_ctx >>= fun () =>
        cleanup_router ctx >>= fun () =>
          Protocol.Zest.close ctx.zmq_ctx |> 
            fun () => exit 0;
};

let unhandled_error e ctx => {
  let msg = Printexc.to_string e;
  let stack = Printexc.get_backtrace ();
  Logger.error_f "unhandled_error" (Printf.sprintf "%s%s" msg stack) >>= 
    fun () => ack (Ack.Code 128) >>= fun resp => Protocol.Zest.send ctx.zmq_ctx resp;
};

let handle_zmq_error ctx e m => {
  /* try and send service unavailable */
  Logger.error_f "handle_zmq_error" m >>=
    fun () => ack (Ack.Code 163) >>= fun resp => Protocol.Zest.send ctx.zmq_ctx resp;
};

exception Interrupt of string;

let register_signal_handlers () => {
  open Lwt_unix;
  on_signal Sys.sigterm (fun _ => raise (Interrupt "Caught SIGTERM")) |>
    fun id => on_signal Sys.sighup (fun _ => raise (Interrupt "Caught SIGHUP")) |>
      fun id => on_signal Sys.sigint (fun _ => raise (Interrupt "Caught SIGINT"));
};

let rec run_server ctx => {
  let _ = try {Lwt_main.run {server ctx}} 
    { 
      | Interrupt m => terminate_server ctx;
      | ZMQ.ZMQ_exception e m => handle_zmq_error ctx e m;
      | e => unhandled_error e ctx;
    };
  run_server ctx;
};

let init zmq_ctx numts_ctx blobts_ctx jsonkv_ctx textkv_ctx binarykv_ctx observe_ctx => {
  observe_ctx: observe_ctx,
  numts_ctx: numts_ctx,
  blobts_ctx: blobts_ctx,
  jsonkv_ctx: jsonkv_ctx,
  textkv_ctx: textkv_ctx,
  binarykv_ctx: binarykv_ctx,
  zmq_ctx: zmq_ctx,
  version: 1
};

let setup_server () => {
  parse_cmdline ();
  !log_mode ? Logger.init () : ();
  setup_router_keys ();
  set_server_key !server_secret_key_file;
  set_token_key !token_secret_key_file;
  let zmq_ctx = Protocol.Zest.create endpoints::(!rep_endpoint, !rout_endpoint) keys::(!server_secret_key, !router_secret_key);
  let numts_ctx = Numeric_timeseries.create path_to_db::!store_directory max_buffer_size::10000 shard_size::1000;
  let jsonkv_ctx = Keyvalue.Json.create path_to_db::!store_directory;
  let textkv_ctx = Keyvalue.Text.create path_to_db::!store_directory;
  let binarykv_ctx = Keyvalue.Binary.create path_to_db::!store_directory;
  let blobts_ctx = Blob_timeseries.create path_to_db::!store_directory max_buffer_size::1000 shard_size::100;
  let observe_ctx = Observe.create ();
  let ctx = init zmq_ctx numts_ctx blobts_ctx jsonkv_ctx textkv_ctx binarykv_ctx observe_ctx;
  let _ = register_signal_handlers ();  
  run_server ctx |> fun () => terminate_server ctx;
};

setup_server ();
