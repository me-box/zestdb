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
  mutable prov_ctx: option Prov.t,
  hc_ctx: Hc.t,
  observe_ctx: Observe.t,
  numts_ctx: Numeric_timeseries.t,
  blobts_ctx: Blob_timeseries.t,
  jsonkv_ctx: Keyvalue.Json.t,
  textkv_ctx: Keyvalue.Text.t,
  binarykv_ctx: Keyvalue.Binary.t,
  zmq_ctx: Protocol.Zest.t, 
  version: int
};

let get_prov_ctx ctx => {
  switch ctx.prov_ctx {
  | Some ctx => ctx;
  | None => failwith "Prov ctx unset";
  };
};

let get_time () => {
  let t_sec = Unix.gettimeofday ();
  let t_ms = t_sec *. 1000.0;
  int_of_float t_ms;
};

let create_audit_payload_worker ctx code resp_code => {
  open Protocol.Zest;
  let prov_ctx = get_prov_ctx ctx;
  let uri_host = Prov.uri_host prov_ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let timestamp = get_time (); 
  let server = !identity;
  create_ack_payload 69 (Printf.sprintf "%d %s %s %s %s %d" timestamp server uri_host code uri_path resp_code);
};

let create_audit_payload ctx status payload => {
  let prov_ctx = get_prov_ctx ctx;
  let meth = Prov.code_as_string prov_ctx;
  switch status {
  | Ack.Code 163 => Some payload;
  | Ack.Code n => Some (create_audit_payload_worker ctx meth n);
  | Ack.Payload _ => Some (create_audit_payload_worker ctx "GET" 69); 
  | Ack.Observe _ => Some (create_audit_payload_worker ctx "GET(OBSERVE)" 69); 
  };
};


let create_data_payload_worker ctx payload => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let content_format = Prov.content_format_as_string prov_ctx;    
  let timestamp = get_time (); 
  let entry = Printf.sprintf "%d %s %s %s" timestamp uri_path content_format payload;
  Protocol.Zest.create_ack_payload 69 entry;
};

let create_data_payload ctx status payload => {
  switch status {
  | Ack.Code 163 => Some payload;
  | Ack.Observe _ => None;
  | Ack.Code 128 => None;
  | Ack.Code 129 => None;
  | Ack.Code 143 => None;
  | Ack.Code 66 => None;
  | Ack.Payload _ => None;
  | Ack.Code _ => Some (create_data_payload_worker ctx payload);
  };
};

let create_router_payload ctx mode status payload => {
  switch mode {
  | "data" => create_data_payload ctx status payload;
  | "audit" => create_audit_payload ctx status payload;
  | _ => Some (Protocol.Zest.create_ack 128);
  };
};

let route_message alist ctx status payload => {
  open Logger;
  let rec loop l => {
    switch l {
      | [] => Lwt.return_unit;
      | [(ident,expiry,mode), ...rest] => {
          switch (create_router_payload ctx mode status payload) {
          | Some payload' => {
              Protocol.Zest.route ctx.zmq_ctx ident payload' >>= fun () =>
                debug_f "routing" (Printf.sprintf "Routing:\n%s \nto ident:%s with expiry:%lu and mode:%s" (to_hex payload') ident expiry mode) >>= 
                  fun () => loop rest;
            }
          | None => loop rest;
          };
        };
      };    
  };
  loop alist;
};

let route status payload ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let key = Prov.ident prov_ctx;
  route_message (Observe.get ctx.observe_ctx key) ctx status payload; 
};

let handle_expire ctx => {
  Observe.expire ctx.observe_ctx >>=
    fun uuids => route_message uuids ctx (Ack.Code 163) (Protocol.Zest.create_ack 163);
};


let handle_get_read_ts_blob_latest id ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_latest ctx::ctx.blobts_ctx id::id);
    | [x, ...xs] => Json (Blob_timeseries.read_latests ctx::ctx.blobts_ctx id_list::[x, ...xs]);
  };
};


let handle_get_read_ts_blob_earliest id ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_earliest ctx::ctx.blobts_ctx id::id);
    | [x, ...xs] => Json (Blob_timeseries.read_earliests ctx::ctx.blobts_ctx id_list::[x, ...xs]);
  };
};

let apply path apply0 apply1 apply2 => {
  open Response;
  open Numeric;
  open Filter;
  switch path {
    | [] => apply0 ();
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


let handle_get_read_ts_numeric_earliest id func ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {   
        let apply0 () => Json (Numeric_timeseries.read_earliest ctx::ctx.numts_ctx id::id fn::[]);
        let apply1 f => Json (Numeric_timeseries.read_earliest ctx::ctx.numts_ctx id::id fn::[f]);
        let apply2 f1 f2 => Json (Numeric_timeseries.read_earliest ctx::ctx.numts_ctx id::id fn::[f1, f2]);
        apply func apply0 apply1 apply2;
      };
    | [x, ...xs] => {
        let apply0 () => Json (Numeric_timeseries.read_earliests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[]);
        let apply1 f => Json (Numeric_timeseries.read_earliests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[f]);
        let apply2 f1 f2 => Json (Numeric_timeseries.read_earliests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[f1, f2]);
        apply func apply0 apply1 apply2;
      };
    };  
};

let handle_get_read_ts_numeric_latest id func ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {
        let apply0 () => Json (Numeric_timeseries.read_latest ctx::ctx.numts_ctx id::id fn::[]);
        let apply1 f => Json (Numeric_timeseries.read_latest ctx::ctx.numts_ctx id::id fn::[f]);
        let apply2 f1 f2 => Json (Numeric_timeseries.read_latest ctx::ctx.numts_ctx id::id fn::[f1, f2]);
        apply func apply0 apply1 apply2; 
      };   
    | [x, ...xs] => {
        let apply0 () => Json (Numeric_timeseries.read_latests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[]);
        let apply1 f => Json (Numeric_timeseries.read_latests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[f]);
        let apply2 f1 f2 => Json (Numeric_timeseries.read_latests ctx::ctx.numts_ctx id_list::[x, ...xs] fn::[f1, f2]);
        apply func apply0 apply1 apply2; 
      };  
    };  
};

let handle_get_read_ts_numeric_last id n func ctx => {
  open Response;
  open Numeric_timeseries;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {
        let apply0 () => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
        let apply1 f => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
        let apply2 f1 f2 => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
        apply func apply0 apply1 apply2;  
      };
    | [x, ...xs] => {
        let apply0 () => Json (read_lasts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[]);
        let apply1 f => Json (read_lasts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[f]);
        let apply2 f1 f2 => Json (read_lasts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[f1, f2]);
        apply func apply0 apply1 apply2;  
      };
    };
};

let handle_get_read_ts_blob_last id n ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_last ctx::ctx.blobts_ctx id::id n::(int_of_string n));
    | [x, ...xs] => Json (Blob_timeseries.read_lasts ctx::ctx.blobts_ctx id_list::[x, ...xs] n::(int_of_string n));
  };
};

let handle_get_read_ts_numeric_first id n func ctx => {
  open Response;
  open Numeric_timeseries;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {
        let apply0 () => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
        let apply1 f => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
        let apply2 f1 f2 => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
        apply func apply0 apply1 apply2;  
      };
    | [x, ...xs] => {
        let apply0 () => Json (read_firsts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[]);
        let apply1 f => Json (read_firsts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[f]);
        let apply2 f1 f2 => Json (read_firsts ctx::ctx.numts_ctx id_list::[x, ...xs] n::(int_of_string n) fn::[f1, f2]);
        apply func apply0 apply1 apply2;  
      };
    };
};

let handle_get_read_ts_blob_first id n ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_first ctx::ctx.blobts_ctx id::id n::(int_of_string n));
    | [x, ...xs] => Json (Blob_timeseries.read_firsts ctx::ctx.blobts_ctx id_list::[x, ...xs] n::(int_of_string n));
  };
};

let handle_get_read_ts_numeric_since id t func ctx => {
  open Response;
  open Numeric_timeseries;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {
        let apply0 () => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[]);
        let apply1 f => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f]);
        let apply2 f1 f2 => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f1, f2]);   
        apply func apply0 apply1 apply2;
      };
    | [x, ...xs] => {
        let apply0 () => Json (read_sinces ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t) fn::[]);
        let apply1 f => Json (read_sinces ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t) fn::[f]);
        let apply2 f1 f2 => Json (read_sinces ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t) fn::[f1, f2]);   
        apply func apply0 apply1 apply2;
      };
    };
};

let handle_get_read_ts_blob_since id t ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_since ctx::ctx.blobts_ctx id::id from::(int_of_string t));
    | [x, ...xs] => Json (Blob_timeseries.read_sinces ctx::ctx.blobts_ctx id_list::[x, ...xs] from::(int_of_string t));
  };
};


let handle_get_read_ts_numeric_range id t1 t2 func ctx => {
  open Response;  
  open Numeric_timeseries;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => {
        let apply0 () => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[]);
        let apply1 f => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f]);
        let apply2 f1 f2 => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f1, f2]);
        apply func apply0 apply1 apply2;
      };
    | [x, ...xs] => {
        let apply0 () => Json (read_ranges ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t1) to::(int_of_string t2) fn::[]);
        let apply1 f => Json (read_ranges ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t1) to::(int_of_string t2) fn::[f]);
        let apply2 f1 f2 => Json (read_ranges ctx::ctx.numts_ctx id_list::[x, ...xs] from::(int_of_string t1) to::(int_of_string t2) fn::[f1, f2]);
        apply func apply0 apply1 apply2;
      };
    };
};

let handle_get_read_ts_blob_range id t1 t2 ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.read_range ctx::ctx.blobts_ctx id::id from::(int_of_string t1) to::(int_of_string t2));
    | [x, ...xs] => Json (Blob_timeseries.read_ranges ctx::ctx.blobts_ctx id_list::[x, ...xs] from::(int_of_string t1) to::(int_of_string t2));
  };
};


let handle_get_read_ts_numeric_length id ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
  | [] => Empty;
  | [id] => Json (Numeric_timeseries.length ctx::ctx.numts_ctx id::id);
  | [x, ...xs] => Json (Numeric_timeseries.lengths ctx::ctx.numts_ctx id_list::[x, ...xs]);
  };
};

let handle_get_read_ts_blob_length id ctx => {
  open Response;
  switch (String.split_on_char ',' id) {
    | [] => Empty;
    | [id] => Json (Blob_timeseries.length ctx::ctx.blobts_ctx id::id);
    | [x, ...xs] => Json (Blob_timeseries.lengths ctx::ctx.blobts_ctx id_list::[x, ...xs]);
  };
};

let handle_get_read_ts uri_path ctx => {
  open List;
  open Response;  
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", "blob", id, "length"] => handle_get_read_ts_blob_length id ctx;
  | ["", "ts", id, "length"] => handle_get_read_ts_numeric_length id ctx;
  | ["", "ts", "blob", id, "latest"] => handle_get_read_ts_blob_latest id ctx;
  | ["", "ts", id, "latest", ...func] => handle_get_read_ts_numeric_latest id func ctx;
  | ["", "ts", "blob", id, "earliest"] => handle_get_read_ts_blob_earliest id ctx;
  | ["", "ts", id, "earliest", ...func] => handle_get_read_ts_numeric_earliest id func ctx;
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
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "kv", id, "keys"] => Json (keys ctx::ctx.jsonkv_ctx id::id);
  | ["", "kv", id, key] => Json (read ctx::ctx.jsonkv_ctx id::id key::key);
  | _ => Empty;
  };
};

let handle_get_read_kv_text uri_path ctx => {
  open Response;
  open Keyvalue.Text;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "kv", id, "keys"] => Json (keys ctx::ctx.textkv_ctx id::id);
  | ["", "kv", id, key]  => Text (read ctx::ctx.textkv_ctx id::id key::key);
  | _ => Empty;
  };
};

let handle_get_read_kv_binary uri_path ctx => {
  open Response;
  open Keyvalue.Binary;
  let path_list = String.split_on_char '/' uri_path;  
  switch path_list {
  | ["", "kv", id, "keys"] => Json (keys ctx::ctx.binarykv_ctx id::id);
  | ["", "kv", id, key] => Text (read ctx::ctx.binarykv_ctx id::id key::key);
  | _ => Empty;
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


let handle_read_hypercat ctx => {
  open Ack;
  Hc.get ctx::ctx.hc_ctx >>=
    fun json => Ezjsonm.to_string json |>
      fun s => (Payload 50 s) |> Lwt.return;
};

let handle_get_read content_format uri_path ctx => {
  switch uri_path {
  | "/cat" => handle_read_hypercat ctx;
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
  let prov_ctx = get_prov_ctx ctx;
  let m = Prov.log_entry prov_ctx;
  let json = to_json payload;
  switch json {
  | Some value => {
      if (is_valid value) {
        Some (write ctx::ctx.numts_ctx timestamp::timestamp info::m id::key json::value);
      } else None;
    };
  | None => None;
  };  
};

let handle_post_write_ts_blob ::timestamp=None key payload ctx => {
  open Blob_timeseries;
  let prov_ctx = get_prov_ctx ctx;
  let m = Prov.log_entry prov_ctx;
  let json = to_json payload;
  switch json {
  | Some value => Some (write ctx::ctx.blobts_ctx timestamp::timestamp info::m id::key json::value);
  | None => None;
  };  
};

let handle_post_write_ts payload ctx => {
  open List;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
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

let handle_post_write_kv_json payload ctx => {
  open Keyvalue.Json;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => switch (to_json payload) {
    | Some json => Some (write ctx::ctx.jsonkv_ctx id::id key::key json::json);
    | None => None;
    }
  | None => None;
  };
};

let handle_post_write_kv_text payload ctx => {
  open Keyvalue.Text;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Some (write ctx::ctx.textkv_ctx id::id key::key text::payload);
  | None => None;
  };
};

let handle_post_write_kv_binary payload ctx => {
  open Keyvalue.Binary;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch (get_id_key "kv" uri_path) {
  | Some (id, key) => Some (write ctx::ctx.binarykv_ctx id::id key::key binary::payload);
  | None => None;
  };
};

let handle_write_database payload ctx => {
  open Ack;
  open Ezjsonm;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let content_format = Prov.content_format prov_ctx;
  let mode = get_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/ts/", 50) => handle_post_write_ts payload ctx;
  | ("/kv/", 50) => handle_post_write_kv_json payload ctx;
  | ("/kv/", 0) => handle_post_write_kv_text payload ctx;
  | ("/kv/", 42) => handle_post_write_kv_binary payload ctx;  
  | _ => None;
  };
  switch result {
  | Some promise => promise >>= fun () => Lwt.return (Code 65);
  | None => Lwt.return (Code 128);
  };
};



let handle_write_hypercat ctx payload => {
  open Ack;
  let json = to_json payload;
  switch json {
  | Some json => {
      Hc.update ctx::ctx.hc_ctx item::json >>=
        fun result => switch result {
        | Ok => (Code 65)
        | Error n => (Code n)
        } |> Lwt.return;
    };
  | None => Lwt.return (Code 128);
  };
};

let handle_post_write payload ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch uri_path {
  | "/cat" => handle_write_hypercat ctx payload;
  | _ => handle_write_database payload ctx; 
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


let handle_get_observed ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let token = Prov.token prov_ctx;
  let content_format = Prov.content_format prov_ctx;      
  if (is_valid_token token uri_path "GET") {
    handle_get_read content_format uri_path ctx >>=
      fun resp => route resp "" ctx >>= 
        fun () => ack resp;
  } else {
    route (Ack.Code 129) "" ctx >>= 
      fun () => ack (Ack.Code 129);
  }; 
};

let handle_get_unobserved ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let token = Prov.token prov_ctx;
  let content_format = Prov.content_format prov_ctx;
  if (is_valid_token token uri_path "GET") {
    handle_get_read content_format uri_path ctx >>= ack;
   } else {
    ack (Ack.Code 129)
  };  
};


let handle_get_observation_request observe_mode ctx options => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let token = Prov.token prov_ctx;
  let content_format = Prov.content_format prov_ctx;
  let max_age = Prov.max_age options;
  if (is_valid_token token uri_path "GET") {
    let uuid = create_uuid ();
    Observe.add ctx.observe_ctx uri_path content_format uuid max_age observe_mode >>=
      fun () => route (Ack.Observe !router_public_key uuid) "" ctx >>=
        fun () => ack (Ack.Observe !router_public_key uuid);
  } else {
    route (Ack.Code 129) "" ctx >>= 
      fun () => ack (Ack.Code 129);
  };
};

let handle_get ctx options => {
  let prov_ctx = get_prov_ctx ctx;
  let key = Prov.ident prov_ctx;
  let observed = Prov.observed options;
  if ((observed == "data") || (observed == "audit")) {
    handle_get_observation_request observed ctx options;
  } else if (Observe.is_observed ctx.observe_ctx key) {
    handle_get_observed ctx;
  } else {
    handle_get_unobserved ctx;
  };
};

let handle_post_unobserved payload ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let token = Prov.token prov_ctx;
  if (is_valid_token token uri_path "POST") {
    handle_post_write payload ctx >>= ack;
  } else {
    ack (Code 129);
  };
};

let handle_post_observed payload ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let token = Prov.token prov_ctx;
  if (is_valid_token token uri_path "POST") {
    handle_post_write payload ctx >>=
      fun resp => route resp payload ctx >>= 
        fun () => ack resp;
   } else {
    route (Ack.Code 129) payload ctx >>= 
      fun () => ack (Code 129);
  };
};

let handle_post ctx payload => {
  let prov_ctx = get_prov_ctx ctx;
  let key = Prov.ident prov_ctx;
  if (Observe.is_observed ctx.observe_ctx key) {
    handle_post_observed payload ctx;
  } else {
    handle_post_unobserved payload ctx;
  };
};


let handle_delete_write_kv_json ctx => {
  open Keyvalue.Json;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", mode, id, key] => Some (delete ctx::ctx.jsonkv_ctx id::id key::key);
  | ["", mode, id] => Some (delete_all ctx::ctx.jsonkv_ctx id::id);
  | _ => None;
  };
};

let handle_delete_write_kv_text ctx => {
  open Keyvalue.Text;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", mode, id, key] => Some (delete ctx::ctx.textkv_ctx id::id key::key);
  | ["", mode, id] => Some (delete_all ctx::ctx.textkv_ctx id::id);
  | _ => None;
  };
};

let handle_delete_write_kv_binary ctx => {
  open Keyvalue.Binary;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", mode, id, key] => Some (delete ctx::ctx.binarykv_ctx id::id key::key);
  | ["", mode, id] => Some (delete_all ctx::ctx.binarykv_ctx id::id);
  | _ => None;
  };
};


let get_id_list uri_path => {
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", "blob", ids, ..._] => String.split_on_char ',' ids;
  | ["", "ts", ids, ..._] => String.split_on_char ',' ids;
  | _ => [];
  };
};

let has_unsupported_delete_api lis => {
  switch lis {
  | [] => false;
  | ["","ts","blob",_,"first", ..._] => true;
  | ["","ts","blob",_,"last", ..._] => true;
  | ["","ts",_,"first", ..._] => true;
  | ["","ts",_,"last", ..._] => true;
  | _ =>  switch (List.rev lis) {
          | [x, ..._] when x == "sum" => true;
          | [x, ..._] when x == "count" => true;
          | [x, ..._] when x == "min" => true;
          | [x, ..._] when x == "max" => true;
          | [x, ..._] when x == "mean" => true;
          | [x, ..._] when x == "median" => true;
          | [x, ..._] when x == "sd" => true;
          | [x, ..._] when x == "length" => true;
          | _ => false;
          };
  };
};


let handle_delete_ts_numeric ctx => {
  open Numeric_timeseries;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch (handle_get_read_ts uri_path ctx) {
  | Json json => Some (delete ctx::ctx.numts_ctx id_list::(get_id_list uri_path) json::json);
  | _ => None;
  };
};

let handle_delete_ts_blob ctx => {
  open Blob_timeseries;
  let prov_ctx = get_prov_ctx ctx;
  let uri_path = Prov.uri_path prov_ctx;
  switch (handle_get_read_ts uri_path ctx) {
  | Json json => Some (delete ctx::ctx.blobts_ctx id_list::(get_id_list uri_path) json::json);
  | _ => None;
  };
};

let handle_delete_write ctx => {
  open Ack;
  open Ezjsonm;
  let prov_ctx = get_prov_ctx ctx;
  let content_format = Prov.content_format prov_ctx;
  let uri_path = Prov.uri_path prov_ctx;
  let path_list = String.split_on_char '/' uri_path;
  if (has_unsupported_delete_api path_list) {
    Lwt.return (Code 134);
  } else {
    let result = switch (path_list, content_format) {
      | (["", "kv", ..._], 50) => handle_delete_write_kv_json ctx;
      | (["", "kv", ..._], 0) => handle_delete_write_kv_text ctx;
      | (["", "kv", ..._], 42) => handle_delete_write_kv_binary ctx;
      | (["", "ts", "blob", ..._], 50) => handle_delete_ts_blob ctx;
      | (["", "ts", ..._], 50) => handle_delete_ts_numeric ctx;
      | _ => None;
      };
      switch result {
      | Some promise => promise >>= fun () => Lwt.return (Code 66);
      | None => Lwt.return (Code 128);
      };
  };
};

let handle_delete_observed ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let token = Prov.token prov_ctx;
  let uri_path = Prov.uri_path prov_ctx;
  if (is_valid_token token uri_path "DELETE") {
    handle_delete_write ctx >>=
      fun resp => route resp "" ctx >>= 
        fun () => ack resp;
  } else {
    route (Ack.Code 129) "" ctx >>= 
      fun () => ack (Code 129);
  };
};


let handle_delete_unobserved ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let token = Prov.token prov_ctx;
  let uri_path = Prov.uri_path prov_ctx;
  if (is_valid_token token uri_path "DELETE") {
    handle_delete_write ctx >>= ack;
  } else {
    ack (Code 129);
  };
};


let handle_delete ctx => {
  let prov_ctx = get_prov_ctx ctx;
  let key = Prov.ident prov_ctx;
  if (Observe.is_observed ctx.observe_ctx key) {
    handle_delete_observed ctx;
  } else {
    handle_delete_unobserved ctx;
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
          ctx.prov_ctx = Some (Prov.create code::code options::options token::token);
          switch code {
          | 1 => handle_get ctx options;
          | 2 => handle_post ctx payload;
          | 4 => handle_delete ctx;
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
    fun uuids => route_message uuids ctx (Ack.Code 163) (Protocol.Zest.create_ack 163) >>=
      fun () => Lwt_unix.sleep 1.0;
};

let terminate_server ctx => {
  Lwt_io.printf "\nShutting down server...\n" >>= fun () =>
    Blob_timeseries.flush ctx::ctx.blobts_ctx info::"terminated" >>= fun () =>
      Numeric_timeseries.flush ctx::ctx.numts_ctx info::"terminated" >>= fun () =>
        cleanup_router ctx >>= fun () =>
          Protocol.Zest.close ctx.zmq_ctx |> 
            fun () => exit 0;
};

let unhandled_error e ctx => {
  let msg = Printexc.to_string e;
  let stack = Printexc.get_backtrace ();
  Logger.error_f "unhandled_error" (Printf.sprintf "%s%s" msg stack) >>= 
    fun () => ack (Ack.Code 160) >>= fun resp => Protocol.Zest.send ctx.zmq_ctx resp;
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
      | ZMQ.ZMQ_exception e m => ack (Ack.Code 163) >>= Protocol.Zest.send ctx.zmq_ctx;
      | Stack_overflow => ack (Ack.Code 141) >>= Protocol.Zest.send ctx.zmq_ctx;
      | e => unhandled_error e ctx;
    };
  run_server ctx;
};

let init zmq_ctx numts_ctx blobts_ctx jsonkv_ctx textkv_ctx binarykv_ctx observe_ctx hc_ctx => {
  prov_ctx: None,
  hc_ctx: hc_ctx,
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
  let hc_ctx = Hc.create store::jsonkv_ctx;
  let ctx = init zmq_ctx numts_ctx blobts_ctx jsonkv_ctx textkv_ctx binarykv_ctx observe_ctx hc_ctx;
  let _ = register_signal_handlers ();  
  run_server ctx |> fun () => terminate_server ctx;
};

setup_server ();
