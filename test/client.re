open Lwt.Infix;

let create_content_format id => {
  let bits = [%bitstring {|id : 16 : bigendian|}];
  Bitstring.string_of_bitstring bits  
};

let req_endpoint = ref "tcp://127.0.0.1:5555";
let deal_endpoint = ref "tcp://127.0.0.1:5556";
let curve_server_key = ref "";
let curve_public_key = ref "";
let curve_secret_key = ref "";
let router_public_key = ref "";
let router_secret_key = ref "";
let token = ref "";
let uri_path = ref "";
let content_format = ref (create_content_format 50);
let max_age = ref 60;
let payload = ref "";
let loop_count = ref 0;
let call_freq = ref 1.0;
let command = ref (fun _ => Lwt.return_unit);
let log_mode = ref false;
let file = ref false;
let version = 1;

module Response = {
    type t = OK | Unavailable |  Payload string | Observe string string | Error string;
};

let setup_logger () => {
  Lwt_log_core.default :=
    Lwt_log.channel
      template::"$(date).$(milliseconds) [$(level)] $(message)"
      close_mode::`Keep
      channel::Lwt_io.stdout
      ();
  Lwt_log_core.add_rule "*" Lwt_log_core.Debug;
};

let to_hex msg => {
  open Hex;
  String.trim (of_string msg |> hexdump_s print_chars::false);
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

let has_public_key options => {
  if (Array.exists (fun (number,_) => number == 2048) options) {
    true;
  } else {
    false;
  }
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

let handle_ack_content options payload => {
  let payload = Bitstring.string_of_bitstring payload;
  if (has_public_key options) {
    let key = get_option_value options 2048;
    Response.Observe key payload |> Lwt.return;
  } else {
    Response.Payload payload |> Lwt.return;    
  };
};

let handle_ack_created options => {
  Response.OK |> Lwt.return;
};

let handle_service_unavailable options => {
  Response.Unavailable |> Lwt.return;
};

let handle_ack_bad_request options => {
  Response.Error "Bad Request" |> Lwt.return;
};

let handle_unsupported_content_format options => {
  Response.Error "Unsupported Content-Format" |> Lwt.return;
};

let handle_ack_unauthorized options => {
  Response.Error "Unauthorized" |> Lwt.return;
};

let handle_response msg => {
  Lwt_log_core.debug_f "Received:\n%s" (to_hex msg) >>=
    fun () => {
      let r0 = Bitstring.bitstring_of_string msg;
      let (tkl, oc, code, r1) = handle_header r0;
      let (options,payload) = handle_options oc r1;
      switch code {
      | 69 => handle_ack_content options payload;
      | 65 => handle_ack_created options;
      | 128 => handle_ack_bad_request options;
      | 129 => handle_ack_unauthorized options;
      | 143 => handle_unsupported_content_format options;
      | 163 => handle_service_unavailable options;
      | _ => failwith ("invalid code:" ^ string_of_int code);
      };
    };  
};

let send_request msg::msg to::socket => {
  Lwt_log_core.debug_f "Sending:\n%s" (to_hex msg) >>=
    fun () =>
      Lwt_zmq.Socket.send socket msg >>=
        fun () =>
          Lwt_zmq.Socket.recv socket >>=
            handle_response;
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

let create_token tk::token => {
  let bit_length = (String.length token) * 8; 
  (token, bit_length);  
};

let create_options options => {
  let count = Array.length options;
  let values = Array.map (fun (x,y) => x) options;
  let value = Bitstring.concat (Array.to_list values);
  let lengths = Array.map (fun (x,y) => y) options;
  let length = Array.fold_left (fun x y => x + y) 0 lengths;
  (value, length, count);
};

let create_post_options uri::uri format::format => {
  let uri_path = create_option number::11 value::uri;
  let uri_host = create_option number::3 value::(Unix.gethostname ());
  let content_format = create_option number::12 value::format;
  create_options [|uri_path, uri_host, content_format|];
};

let create_get_options uri::uri format::format => {
  let uri_path = create_option number::11 value::uri;
  let uri_host = create_option number::3 value::(Unix.gethostname ());
  let content_format = create_option number::12 value::format;  
  create_options [|uri_path, uri_host, content_format|];
};

let create_observe_option_max_age seconds => {
  let bits = [%bitstring {|seconds : 32 : bigendian|}];
  Bitstring.string_of_bitstring bits  
};

let create_max_age seconds => {
  let bits = [%bitstring {|seconds : 32 : bigendian|}];
  Bitstring.string_of_bitstring bits  
};

let create_observe_options ::format=(!content_format) ::age=(!max_age) uri::uri => {
  let uri_path = create_option number::11 value::uri;
  let uri_host = create_option number::3 value::(Unix.gethostname ());
  let content_format = create_option number::12 value::format;    
  let observe = create_option number::6 value::"";
  let max_age = create_option number::14 value::(create_max_age (Int32.of_int age));
  create_options [|uri_path, uri_host, observe, content_format, max_age|];
};

let post ::token=(!token) ::format=(!content_format) uri::uri payload::payload () => {
  let (options_value, options_length, options_count) = create_post_options uri::uri format::format;
  let (header_value, header_length) = create_header tkl::(String.length token) oc::options_count code::2;
  let (token_value, token_length) = create_token tk::token;
  let payload_length = String.length payload * 8;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      token_value : token_length : string; 
      options_value : options_length : bitstring;
      payload : payload_length : string
    |}
  ];
  Bitstring.string_of_bitstring bits;
};

let get ::token=(!token) ::format=(!content_format) uri::uri () => {
  let (options_value, options_length, options_count) = create_get_options uri::uri format::format;
  let (header_value, header_length) = create_header tkl::(String.length token) oc::options_count code::1;
  let (token_value, token_length) = create_token tk::token;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      token_value : token_length : string; 
      options_value : options_length : bitstring
    |}
  ];
  Bitstring.string_of_bitstring bits;
};

let observe ::token=(!token) ::format=(!content_format) uri::uri () => {
  let (options_value, options_length, options_count) = create_observe_options age::!max_age uri::uri format::format;
  let (header_value, header_length) = create_header tkl::(String.length token) oc::options_count code::1;
  let (token_value, token_length) = create_token tk::token;
  let bits = [%bitstring 
    {|header_value : header_length : bitstring;
      token_value : token_length : string; 
      options_value : options_length : bitstring
    |}
  ];
  Bitstring.string_of_bitstring bits;
};

let set_main_socket_security soc => {
  ZMQ.Socket.set_curve_serverkey soc !curve_server_key;
  ZMQ.Socket.set_curve_publickey soc !curve_public_key;
  ZMQ.Socket.set_curve_secretkey soc !curve_secret_key;
};

let set_dealer_socket_security soc key=> {
  ZMQ.Socket.set_curve_serverkey soc key;
  ZMQ.Socket.set_curve_publickey soc !router_public_key;
  ZMQ.Socket.set_curve_secretkey soc !router_secret_key;
};

let connect_request_socket endpoint ctx kind => {
  let soc = ZMQ.Socket.create ctx kind;
  set_main_socket_security soc;
  ZMQ.Socket.connect soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let connect_dealer_socket key ident endpoint ctx kind => {
  let soc = ZMQ.Socket.create ctx kind;
  set_dealer_socket_security soc key;
  ZMQ.Socket.set_identity soc ident; 
  ZMQ.Socket.connect soc endpoint;
  Lwt_zmq.Socket.of_socket soc;
};

let close_socket lwt_soc => {
  let soc = Lwt_zmq.Socket.to_socket lwt_soc;
  ZMQ.Socket.close soc;
};

let post_loop socket count => {
  let rec loop n => {
    send_request msg::(post uri::!uri_path payload::!payload ()) to::socket >>=
      fun resp =>
        switch resp {
        | Response.OK => {
            Lwt_io.printf "=> Created\n" >>=
              fun () =>
                if (n > 1) {
                  Lwt_unix.sleep !call_freq >>= fun () => loop (n - 1);
                } else { 
                  Lwt.return_unit; 
                };
          };
        | Response.Error msg => Lwt_io.printf "=> %s\n" msg; 
        | Response.Unavailable => Lwt_io.printf "=> server unavailable\n";
        | _ => failwith "unhandled response";
        };
  };
  loop count;
};


let post_test ctx => {
  let req_soc = connect_request_socket !req_endpoint ctx ZMQ.Socket.req;
  post_loop req_soc !loop_count >>=
    fun () => close_socket req_soc |> Lwt.return;
};

let get_loop socket count => {
  let rec loop n => {
    send_request msg::(get uri::!uri_path ()) to::socket >>=
      fun resp =>
        switch resp {
        | Response.Payload msg => {
            Lwt_io.printf "%s\n" msg >>=
              fun () =>
                if (n > 1) {
                  Lwt_unix.sleep !call_freq >>= fun () => loop (n - 1);
                } else { 
                  Lwt.return_unit; 
                };
          };
        | Response.Error msg => Lwt_io.printf "=> %s\n" msg;
        | Response.Unavailable => Lwt_io.printf "=> server unavailable\n";
        | _ => failwith "unhandled response";
        };

  };
  loop count;
};

let get_test ctx => {
  let req_soc = connect_request_socket !req_endpoint ctx ZMQ.Socket.req;
  get_loop req_soc !loop_count >>=
    fun () => close_socket req_soc |> Lwt.return;
};

let string_split_at s n =>
  (String.sub s 0 n, String.sub s n (String.length s - n));

let string_drop_prefix n s =>
  string_split_at s n |> snd;


let observe_loop socket count => {
  let rec loop () => {
    Lwt_zmq.Socket.recv socket >>=
      handle_response >>=
        fun resp =>
          switch resp {
          | Response.Payload msg => 
              Lwt_io.printf "%s\n" msg >>=
                fun () => loop ();
          | Response.Unavailable => Lwt_io.printf "=> observation ended\n";
          | _ => failwith "unhandled response";
          };
  };
  loop ();
};


let set_socket_subscription socket path => {
  let soc = Lwt_zmq.Socket.to_socket socket;
  ZMQ.Socket.subscribe soc path;
};

let observe_test ctx => {
  let req_soc = connect_request_socket !req_endpoint ctx ZMQ.Socket.req;
  Lwt_log_core.debug_f "Subscribing:%s" !uri_path >>=
    fun () => 
      send_request msg::(observe uri::!uri_path ()) to::req_soc >>=
        fun resp =>
          switch resp {
          | Response.Observe key ident => {
              Lwt_log_core.debug_f "Observing:%s with ident:%s" !uri_path ident >>=
                fun () => { 
                  close_socket req_soc;
                  let deal_soc = connect_dealer_socket key ident !deal_endpoint ctx ZMQ.Socket.dealer;
                  observe_loop deal_soc !loop_count >>=
                    fun () => close_socket deal_soc |> Lwt.return;
                };
            };
          | Response.Error msg => Lwt_io.printf "=> %s\n" msg;
          | _ => failwith "unhandled response";
          };

};

let handle_mode mode => {
  let func = switch mode {
  | "post" => post_test;
  | "get" => get_test;
  | "observe" => observe_test;
  | _ => raise (Arg.Bad "Unsupported mode");
  };
  command := func;
};

let set_payload_from file => {
  let data = Fpath.v file |>
    Bos.OS.File.read |>
      Rresult.R.get_ok;
  payload := data;
};

let handle_format format => {
  let id = switch format {
  | "text" => 0;
  | "json" => 50;
  | "binary" => 42;
  | _ => raise (Arg.Bad "Unsupported format");
  };
  content_format := create_content_format id;
};

let parse_cmdline () => { 
  let usage = "usage: " ^ Sys.argv.(0);
  let speclist = [
    ("--request-endpoint", Arg.Set_string req_endpoint, ": to set the request/reply endpoint"),
    ("--router-endpoint", Arg.Set_string deal_endpoint, ": to set the router/dealer endpoint"),
    ("--server-key", Arg.Set_string curve_server_key, ": to set the curve server key"),
    ("--public-key", Arg.Set_string curve_public_key, ": to set the curve public key"),
    ("--secret-key", Arg.Set_string curve_secret_key, ": to set the curve secret key"),
    ("--token", Arg.Set_string token, ": to set access token"),
    ("--path", Arg.Set_string uri_path, ": to set the uri path"),
    ("--payload", Arg.Set_string payload, ": to set the message payload"),
    ("--format", Arg.Symbol ["text", "json", "binary"] handle_format, ": to set the message content type"),
    ("--loop", Arg.Set_int loop_count, ": to set the number of times to run post/get/observe test"),
    ("--freq", Arg.Set_float call_freq, ": to set the number of seconds to wait between each get/post operation"),
    ("--mode", Arg.Symbol ["post", "get", "observe"] handle_mode, " : to set the mode of operation"),
    ("--file", Arg.Set file, ": payload contents comes from a file"),
    ("--max-age", Arg.Set_int max_age, ": time in seconds to observe a path"),
    ("--enable-logging", Arg.Set log_mode, ": turn debug mode on"),
  ];
  Arg.parse speclist (fun err => raise (Arg.Bad ("Bad argument : " ^ err))) usage;
};

/* server_key: qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s? */
/* public_key: MP9pZzG25M2$.a%[DwU$OQ#-:C}Aq)3w*<AY^%V{ */
/* secret_key: j#3yqGG17QNTe(g@jJt6[LOg%ivqr<:}L%&NAUPt */  

let setup_curve_keys () => {
  let (public_key,private_key) = ZMQ.Curve.keypair ();
  curve_public_key := public_key;
  curve_secret_key := private_key;
};

let setup_router_keys () => {
  let (public_key,private_key) = ZMQ.Curve.keypair ();
  router_public_key := public_key;
  router_secret_key := private_key;
};

let report_error e => {
  let msg = Printexc.to_string e;
  let stack = Printexc.get_backtrace ();
  let _ = Lwt_log_core.error_f "Opps: %s%s" msg stack;
};

let client () => {
  let ctx = ZMQ.Context.create ();
  setup_curve_keys ();
  setup_router_keys ();
  parse_cmdline ();
  /* can take payload from a file */
  !file ? set_payload_from !payload : ();
  /* log to screen debug info */  
  !log_mode ? setup_logger () : ();
  Lwt_main.run {ctx |> !command};
  ZMQ.Context.terminate ctx;
};

let _ = try (Lwt_main.run {Lwt.return (client ())}) {
  | Invalid_argument msg => Printf.printf "Error: file not found!\n";
  | e => report_error e;
};
