open Lwt.Infix;
let req_endpoint = ref "tcp://127.0.0.1:5555";
let sub_endpoint = ref "tcp://127.0.0.1:5556";
let curve_server_key = ref "";
let curve_public_key = ref "";
let curve_secret_key = ref "";
let token = ref "";
let uri_path = ref "";
let content_format = ref "2"; /* ascii equivalent of 50 representing json */
let payload = ref "";
let loop_count = ref 0;
let call_freq = ref 1.0;
let command = ref (fun _ => Lwt.return_unit);

let handle_header bits => {
  let tuple = [%bitstring
    switch bits {
    | {|tkl : 4 : unsigned;
        oc : 4 : unsigned; 
        code : 8 : unsigned; 
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
    | {|number : 4 : unsigned; 
        len : 16 : bigendian;
        0xf : 4 : unsigned; 
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
      let _ = Lwt_io.printf "option => %d:%s\n" number value;
      handle (oc - 1) r
  };
  (options, handle oc bits);
};

let handle_ack_content options payload => {
  Bitstring.string_of_bitstring payload;
};

let handle_ack_created options => {
  "ok";
};

let handle_response msg => {
  Lwt_io.printlf "Received: %s" msg >>=
    fun () => {
      let r0 = Bitstring.bitstring_of_string msg;
      let (tkl, oc, code, r1) = handle_header r0;
      let (options,payload) = handle_options oc r1;
      let resp = {
        switch code {
        | 69 => handle_ack_content options payload;
        | 65 => handle_ack_created options;
        | _ => failwith "invalid code";
        };
      };
      resp |> Lwt.return;
    };  
};

let send_request msg::msg to::socket => {
  Lwt_io.printf "Sending: %s\n" msg >>=
    fun () =>
      Lwt_zmq.Socket.send socket msg >>=
        fun () =>
          Lwt_zmq.Socket.recv socket >>=
            handle_response;
};

let create_header tkl::tkl oc::oc code::code => {
  let bits = [%bitstring 
    {|tkl : 4 : unsigned;
      oc : 4 : unsigned;
      code : 8 : unsigned
    |}
  ];
  (bits, 16);
};

let create_option number::number value::value => {
  let byte_length = String.length value;
  let bit_length = byte_length * 8;
  let bits = [%bitstring 
    {|number : 4 : unsigned;
      byte_length : 16 : bigendian;
      0xf : 4 : unsigned;
      value : bit_length : string
    |}
  ];
  (bits ,(bit_length+24));
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

let create_get_options uri::uri => {
  let uri_path = create_option number::11 value::uri;
  let uri_host = create_option number::3 value::(Unix.gethostname ());
  create_options [|uri_path, uri_host|];
};

let create_observe_options uri::uri => {
  let uri_path = create_option number::11 value::uri;
  let uri_host = create_option number::3 value::(Unix.gethostname ());
  let observe = create_option number::6 value::"";
  create_options [|uri_path, uri_host, observe|];
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

let get ::token=(!token) uri::uri () => {
  let (options_value, options_length, options_count) = create_get_options uri::uri;
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

let observe ::token=(!token) uri::uri () => {
  let (options_value, options_length, options_count) = create_observe_options uri::uri;
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

let connect_socket endpoint ctx kind => {
  let soc = ZMQ.Socket.create ctx kind; 
  ZMQ.Socket.set_curve_serverkey soc !curve_server_key;
  ZMQ.Socket.set_curve_publickey soc !curve_public_key;
  ZMQ.Socket.set_curve_secretkey soc !curve_secret_key;
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
        Lwt_io.printf "response:%s\n" resp >>=
          fun () =>
            if (n > 1) {
              Lwt_unix.sleep !call_freq >>= fun () => loop (n - 1);
            } else { 
              Lwt.return_unit; 
            };
  };
  loop count;
};


let post_test ctx => {
  let req_soc = connect_socket !req_endpoint ctx ZMQ.Socket.req;
  post_loop req_soc !loop_count >>=
    fun () => close_socket req_soc |> Lwt.return;
};

let get_loop socket count => {
  let rec loop n => {
    send_request msg::(get uri::!uri_path ()) to::socket >>=
      fun resp =>
        Lwt_io.printf "response:%s\n" resp >>=
          fun () =>
            if (n > 1) {
              Lwt_unix.sleep !call_freq >>= fun () => loop (n - 1);
            } else { 
              Lwt.return_unit; 
            };
  };
  loop count;
};

let get_test ctx => {
  let req_soc = connect_socket !req_endpoint ctx ZMQ.Socket.req;
  get_loop req_soc !loop_count >>=
    fun () => close_socket req_soc |> Lwt.return;
};

let string_split_at s n =>
  (String.sub s 0 n, String.sub s n (String.length s - n));

let string_drop_prefix n s =>
  string_split_at s n |> snd;

let observe_loop socket count => {
  let rec loop n => {
    Lwt_zmq.Socket.recv socket >>=
      fun resp =>
        /* remove subscription path prefix */
        string_drop_prefix ((String.length !uri_path)+1) resp |>
          Lwt_io.printf "response:%s\n" >>=
            fun () => 
              (n > 1) ? loop (n-1) : Lwt.return_unit;
  };
  loop count;
};


let set_socket_subscription socket path => {
  let soc = Lwt_zmq.Socket.to_socket socket;
  ZMQ.Socket.subscribe soc path;
};

let observe_test ctx => {
  let req_soc = connect_socket !req_endpoint ctx ZMQ.Socket.req;
  Lwt_io.printf "Subscribing to %s\n" !uri_path >>=
    fun () => 
      send_request msg::(observe uri::!uri_path ()) to::req_soc >>=
        fun resp =>
          Lwt_io.printf "got:%s\n" resp >>=
            fun () => { 
              close_socket req_soc;
              let sub_soc = connect_socket !sub_endpoint ctx ZMQ.Socket.sub;
              set_socket_subscription sub_soc !uri_path;
              observe_loop sub_soc !loop_count >>=
                fun () => close_socket sub_soc |> Lwt.return;
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

let create_content_format id => {
  /* restrict content format support to 1 byte */
  assert ((id >= 0) && (id <= 255));
  let bits = [%bitstring {|id : 8 : unsigned|}];
  Bitstring.string_of_bitstring bits  
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
    ("--subscribe-endpoint", Arg.Set_string sub_endpoint, ": to set the subscribe endpoint"),
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

  ];
  Arg.parse speclist (fun err => raise (Arg.Bad ("Bad argument : " ^ err))) usage;
};

/* server_key: qDq63cJF5gd3Jed:/3t[F8u(ETeep(qk+%pmj(s? */
/* public_key: MP9pZzG25M2$.a%[DwU$OQ#-:C}Aq)3w*<AY^%V{ */
/* secret_key: j#3yqGG17QNTe(g@jJt6[LOg%ivqr<:}L%&NAUPt */  

let ctx = ZMQ.Context.create ();
parse_cmdline ();
Lwt_main.run {ctx |> !command};
ZMQ.Context.terminate ctx;