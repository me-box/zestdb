open Lwt.Infix;

let rep_endpoint = ref "tcp://0.0.0.0:5555";
let rout_endpoint = ref "tcp://0.0.0.0:5556";
/* let notify_list  = ref [(("",0),[("", Int32.of_int 0)])]; */
let notify_list  = ref [];
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


type t = {
  numts_ctx: Numeric_timeseries.t,
  zmq_ctx: Protocol.Zest.t, 
  version: int
};

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


let expire l t => {
  open List;
  let f x =>
    switch x {
    | (k,v) => (k, filter (fun (_,t') => (t' > t) || (t' == Int32.of_int 0)) v);
    };
  filter (fun (x,y) => y != []) (map f l);
};

let diff l1 l2 => List.filter (fun x => not (List.mem x l2)) l1;

let list_uuids alist => {
  open List;  
  map (fun (x,y) => hd y) alist;    
};

let route_message alist ctx payload => {
  let rec loop l => {
    switch l {
      | [] => Lwt.return_unit;
      | [(ident,expiry), ...rest] => {
          Protocol.Zest.route ctx.zmq_ctx ident payload >>=
          /*Lwt_zmq.Socket.send_all socket [ident, payload] >>=*/
            fun _ => Lwt_log_core.debug_f "Routing:\n%s \nto ident:%s with expiry:%lu" (to_hex payload) ident expiry >>=
              fun _ => loop rest;
        };
      };    
  };
  loop alist;
};

let handle_expire ctx => {
  if (observed_paths_exist ()) {
    open Lwt_zmq.Socket.Router;
    let new_notify_list = expire !notify_list (time_now ());
    let uuids = diff (list_uuids !notify_list) (list_uuids new_notify_list);
    notify_list := new_notify_list;
    /* send Service Unavailable */
    route_message uuids ctx (create_ack 163);
  } else {
    Lwt.return_unit;
  };
};

let route tuple payload ctx => {
  let (_,content_format) = tuple;
  route_message (get_ident tuple) ctx (create_ack_payload content_format payload);
};


let handle_get_read_ts_numeric_latest id ctx => {
  open Common.Response;  
  Json (Numeric_timeseries.read_latest ctx::ctx.numts_ctx id::id fn::[]);
};


let handle_get_read_ts_numeric_earliest id ctx => {
  open Common.Response;  
  Json (Numeric_timeseries.read_earliest ctx::ctx.numts_ctx id::id fn::[]);
};


let handle_get_read_ts_numeric_last id n func ctx => {
  open Common.Response;
  open Numeric_timeseries;
  open Numeric;
  open Filter;
  let apply0 = Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
  let apply1 f => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
  let apply2 f1 f2 => Json (read_last ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
  switch func {
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


let handle_get_read_ts_numeric_first id n func ctx => {
  open Common.Response;
  open Numeric_timeseries;
  open Numeric;
  open Filter;
  let apply0 = Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[]);
  let apply1 f => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f]);
  let apply2 f1 f2 => Json (read_first ctx::ctx.numts_ctx id::id n::(int_of_string n) fn::[f1, f2]);
  switch func {
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


let handle_get_read_ts_numeric_since id t func ctx => {
  open Common.Response;
  open Numeric_timeseries;
  open Numeric;
  open Filter;
  let apply0 = Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[]);
  let apply1 f => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f]);
  let apply2 f1 f2 => Json (read_since ctx::ctx.numts_ctx id::id from::(int_of_string t) fn::[f1, f2]);
  switch func {
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
  | ["filter", s1, "contains", s2, "count"] => apply2 (equals s1 s2) count; 
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


let handle_get_read_ts_numeric_range id t1 t2 func ctx => {
  open Common.Response;  
  open Numeric_timeseries;
  open Numeric;
  open Filter;
  let apply0 = Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[]);
  let apply1 f => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f]);
  let apply2 f1 f2 => Json (read_range ctx::ctx.numts_ctx id::id from::(int_of_string t1) to::(int_of_string t2) fn::[f1, f2]);
  switch func {
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

let handle_get_read_ts uri_path ctx => {
  open List;
  open Common.Response;  
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", id, "latest"] => handle_get_read_ts_numeric_latest id ctx;
  | ["", "ts", id, "earliest"] => handle_get_read_ts_numeric_earliest id ctx;
  | ["", "ts", id, "last", n, ...func] => handle_get_read_ts_numeric_last id n func ctx;
  | ["", "ts", id, "first", n, ...func] => handle_get_read_ts_numeric_first id n func ctx;
  | ["", "ts", id, "since", t, ...func] => handle_get_read_ts_numeric_since id t func ctx;
  | ["", "ts", id, "range", t1, t2, ...func] => handle_get_read_ts_numeric_range id t1 t2 func ctx;
  | _ => Empty;
  };
};


let get_key mode uri_path => {
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", mode, key] => Some key; 
  | _ => None;
  };
};

let get_mode uri_path => {
  Str.first_chars uri_path 4;
};




let handle_read_database content_format uri_path ctx => {
  open Common.Ack;
  open Common.Response;
  let mode = get_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/ts/", 50) => handle_get_read_ts uri_path ctx;
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
  open Common.Ack;
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




let handle_post_write_ts_simple ::timestamp=None key payload ctx => {
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

let handle_post_write_ts uri_path payload ctx => {
  open List;
  let path_list = String.split_on_char '/' uri_path;
  switch path_list {
  | ["", "ts", key] => 
    handle_post_write_ts_simple key payload ctx;
  | ["", "ts", key, "at", ts] => 
    handle_post_write_ts_simple timestamp::(Some (int_of_string ts)) key payload ctx;
  | _ => None;
  };
};


  

let handle_write_database content_format uri_path payload ctx => {
  open Common.Ack;
  open Ezjsonm;
  let mode = get_mode uri_path;
  let result = switch (mode, content_format) {
  | ("/ts/", 50) => handle_post_write_ts uri_path payload ctx;  
  | _ => None;
  };
  switch result {
  | Some promise => promise >>= fun () => Lwt.return (Code 65);
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

let handle_post_write content_format uri_path payload ctx => {
  switch uri_path {
  | "/cat" => handle_write_hypercat payload;
  | _ => handle_write_database content_format uri_path payload ctx; 
  };
};

let ack kind => {
  open Common.Ack;
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

let handle_content_format options => {
  let content_format = Protocol.Zest.get_content_format options;
  let _ = Lwt_log_core.debug_f "content_format => %d" content_format;
  content_format;
};

let handle_max_age options => {
  let max_age = Protocol.Zest.get_max_age options;
  let _ = Lwt_log_core.debug_f "max_age => %lu" max_age;
  max_age;
};

let handle_get options token ctx => {
  open Common.Ack;
  let content_format = handle_content_format options;
  let uri_path = Protocol.Zest.get_option_value options 11;
  if ((is_valid_token token uri_path "GET") == false) {
    ack (Code 129)
  } else if (has_observed options) {
    let max_age = handle_max_age options;  
    let uuid = create_uuid ();
    add_to_observe uri_path content_format uuid max_age;
    ack (Observe !router_public_key uuid);
  } else {
    handle_get_read content_format uri_path ctx >>= ack;
  };
};


let handle_post options token payload ctx => {
  open Common.Ack;
  let content_format = handle_content_format options;
  let uri_path = get_option_value options 11;
  let tuple = (uri_path, content_format);
  if ((is_valid_token token uri_path "POST") == false) {
    ack (Code 129);
  } else if (is_observed tuple) {
      handle_post_write content_format uri_path payload ctx >>=
        fun resp => {
          /* we dont want to route bad requests */
          if (resp != (Code 128)) {
            route tuple payload ctx >>= fun () => ack resp;
          } else {
            ack resp;
          };
      };
  } else {
    handle_post_write content_format uri_path payload ctx >>= ack;
  };
};

let handle_msg msg ctx => {
  handle_expire ctx >>=
    fun () =>
      Lwt_log_core.debug_f "Received:\n%s" (to_hex msg) >>=
        fun () => {
          let r0 = Bitstring.bitstring_of_string msg;
          let (tkl, oc, code, r1) = Protocol.Zest.handle_header r0;
          let (token, r2) = Protocol.Zest.handle_token r1 tkl;
          let (options,r3) = Protocol.Zest.handle_options oc r2;
          let payload = Bitstring.string_of_bitstring r3;
          switch code {
          | 1 => handle_get options token ctx;
          | 2 => handle_post options token payload ctx;
          | _ => failwith "invalid code";
          };
        };  
};

let server ctx => {
  let rec loop () => {
    Protocol.Zest.recv ctx.zmq_ctx >>=
      fun msg =>
        handle_msg msg ctx >>=
          fun resp =>
            Protocol.Zest.send ctx.zmq_ctx resp >>=
              fun () =>
                Lwt_log_core.debug_f "Sending:\n%s" (to_hex resp) >>=
                  fun () => loop ();
  };
  Lwt_log_core.info_f "Ready to receive..." >>= fun () => loop ();
};

let setup_rep_socket endpoint ctx kind secret => {
  open ZMQ.Socket;
  let soc = ZMQ.Socket.create ctx kind;
  ZMQ.Socket.set_receive_high_water_mark soc 1;
  set_linger_period soc 0;
  set_curve_server soc true;
  set_curve_secretkey soc secret; 
  bind soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let setup_rout_socket endpoint ctx kind secret => {
  open ZMQ.Socket;
  let soc = ZMQ.Socket.create ctx kind;
  ZMQ.Socket.set_receive_high_water_mark soc 1;
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

let terminate_server ctx => {
  Lwt_io.printf "\nShutting down server...\n" >>= fun () =>
    Numeric_timeseries.flush ctx::ctx.numts_ctx >>= fun () => {
      Protocol.Zest.close ctx.zmq_ctx;
      exit 0;
    };
};

let report_error e ctx => {
  let msg = Printexc.to_string e;
  let stack = Printexc.get_backtrace ();
  Lwt_log_core.error_f "Opps: %s%s" msg stack >>= fun () => 
    ack (Common.Ack.Code 128) >>= fun resp => Protocol.Zest.send ctx.zmq_ctx resp;
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
      | e => report_error e ctx;
    };
  run_server ctx;
};

let init zmq_ctx numts_ctx => {
  numts_ctx: numts_ctx,
  zmq_ctx: zmq_ctx,
  version: 1
};

let setup_server () => {
  parse_cmdline ();
  !log_mode ? setup_logger () : ();
  setup_router_keys ();
  set_server_key !server_secret_key_file;
  set_token_key !token_secret_key_file;
  let zmq_ctx = Protocol.Zest.create endpoints::(!rep_endpoint, !rout_endpoint) keys::(!server_secret_key, !router_secret_key);
  let num_ts = Numeric_timeseries.create path_to_db::!store_directory max_buffer_size::10000 shard_size::1000;
  let ctx = init zmq_ctx num_ts;
  let _ = register_signal_handlers ();  
  run_server ctx |> fun () => terminate_server ctx;
};

setup_server ();

