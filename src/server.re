open Lwt.Infix;

let rep_endpoint = ref("tcp://0.0.0.0:5555");

let rout_endpoint = ref("tcp://0.0.0.0:5556");

/* let notify_list  = ref [(("",0),[("", Int32.of_int 0)])]; */
let token_secret_key_file = ref("");

let token_secret_key = ref("");

let router_public_key = ref("");

let router_secret_key = ref("");

let log_mode = ref(false);

let server_secret_key_file = ref("");

let server_secret_key = ref("");

let version = 1;

let identity = ref(Unix.gethostname());

let content_format = ref("");

/* create stores in local directory by default */
let store_directory = ref("./");

let get_time = () => {
  let t_sec = Unix.gettimeofday();
  let t_ms = t_sec *. 1000.0;
  int_of_float(t_ms);
};

let start_time = ref(get_time());

module Ack = {
  type t =
    | Code(int)
    | Payload(int, string)
    | Observe(string, string)
    | Notify(string);
};

module Response = {
  type t =
    | Empty
    | Json(Lwt.t(Ezjsonm.t))
    | Text(Lwt.t(string))
    | Binary(Lwt.t(string));
};

type t = {
  hc_ctx: Hc.t,
  observe_ctx: Observe.t,
  notify_ctx: Notify.t,
  numts_ctx: Numeric_timeseries.t,
  blobts_ctx: Blob_timeseries.t,
  jsonkv_ctx: Keyvalue.Json.t,
  textkv_ctx: Keyvalue.Text.t,
  binarykv_ctx: Keyvalue.Binary.t,
  zmq_ctx: Protocol.Zest.t,
  version: int
};

let uptime = () => {
  open Ezjsonm;
  let t = get_time() - start_time^;
  dict([("uptime", int(t))]) |> Lwt.return;
};

let create_audit_payload_worker = (prov, code, resp_code) => {
  open Protocol.Zest;
  let uri_host = Prov.uri_host(prov);
  let uri_path = Prov.uri_path(prov);
  let content_format = Prov.content_format(prov);
  let timestamp = get_time();
  let server = identity^;
  create_ack_payload(
    content_format,
    Printf.sprintf(
      "%d %s %s %s %s %d",
      timestamp,
      server,
      uri_host,
      code,
      uri_path,
      resp_code
    )
  );
};

let create_audit_payload = (prov, status, payload) =>
  switch prov {
  | Some(prov') =>
    let meth = Prov.code_as_string(prov');
    switch status {
    | Ack.Code(163) => Some(payload)
    | Ack.Code(n) => Some(create_audit_payload_worker(prov', meth, n))
    | Ack.Payload(_) => Some(create_audit_payload_worker(prov', meth, 69))
    | Ack.Observe(_) => Some(create_audit_payload_worker(prov', "GET(OBSERVE)", 69))
    | Ack.Notify(_) => Some(create_audit_payload_worker(prov', "GET(NOTIFICATION)", 65))
    };
  | None => Some(payload)
  };

let create_data_payload_worker = (prov, payload) =>
  switch prov {
  | Some(prov') =>
    let uri_path = Prov.uri_path(prov');
    let content_format_as_string = Prov.content_format_as_string(prov');
    let content_format = Prov.content_format(prov');
    let timestamp = get_time();
    let entry =
      Printf.sprintf(
        "%d %s %s %s",
        timestamp,
        uri_path,
        content_format_as_string,
        payload
      );
    Protocol.Zest.create_ack_payload(content_format, entry);
  | None => Protocol.Zest.create_ack(163)
  };

let create_data_payload = (prov, status, payload) => {
  switch status {
  | Ack.Code(163) => Some(payload)
  | Ack.Observe(_) => None
  | Ack.Notify(_) => None
  | Ack.Code(128) => None
  | Ack.Code(129) => None
  | Ack.Code(143) => None
  | Ack.Code(66) => None
  | Ack.Payload(_) when payload == "" => None
  | Ack.Payload(_) => Some(create_data_payload_worker(prov, payload))
  | Ack.Code(_) => Some(create_data_payload_worker(prov, payload))
  };
}; 

let create_notification_payload_worker = (prov, payload) => {
  switch prov {
  | Some(prov') =>
    let uri_path = Prov.uri_path(prov');
    let uri_host = Prov.uri_host(prov');
    let callback_uri_path = Str.replace_first(Str.regexp("request"), "response", uri_path);
    let content_format_as_string = Prov.content_format_as_string(prov');
    let content_format = Prov.content_format(prov');
    let timestamp = get_time();
    let entry =
      Printf.sprintf(
        "%d %s %s %s %s",
        timestamp,
        uri_host,
        callback_uri_path,
        content_format_as_string,
        payload
      );
    Protocol.Zest.create_ack_payload(content_format, entry);
  | None => Protocol.Zest.create_ack(163)
  };
};

let create_notification_payload = (prov, status, payload) => {
  switch status {
  | Ack.Code(163) => Some(payload)
  | Ack.Code(65) => Some(create_notification_payload_worker(prov, payload)); 
  | _ => None;
  }
};  

let create_router_payload = (prov, mode, status, payload) =>
  switch mode {
  | "data" => create_data_payload(prov, status, payload)
  | "audit" => create_audit_payload(prov, status, payload)
  | "notification" => create_notification_payload(prov, status, payload)
  | _ => Some(Protocol.Zest.create_ack(128))
  };

let route_message = (alist, ctx, status, payload, prov) => {
  open Logger;
  let rec loop = l =>
    switch l {
    | [] => Lwt.return_unit
    | [(ident, expiry, mode), ...rest] =>
      switch (create_router_payload(prov, mode, status, payload)) {
      | Some(payload') => Protocol.Zest.route(ctx.zmq_ctx, ident, payload') >>= 
          () => debug_f("routing", Printf.sprintf("Routing:\n%s \nto ident:%s with expiry:%lu and mode:%s", to_hex(payload'), ident, expiry, mode)) >>= 
            () => loop(rest)
      | None => loop(rest)
      }
    };
  loop(alist);
};

let route = (status, payload, ctx, prov) => {
  let key = Prov.ident(prov);
  route_message(Observe.get(ctx.observe_ctx, key), ctx, status, payload, Some(prov));
};

let handle_expire = ctx => {
  Observe.expire(ctx.observe_ctx) >>=
    uuids => route_message(uuids, ctx, Ack.Code(163), Protocol.Zest.create_ack(163), None)
};

let get_id_list = uri_path => {
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "ts", "blob", ids, ..._] => String.split_on_char(',', ids)
  | ["", "ts", ids, ..._] => String.split_on_char(',', ids)
  | _ => []
  };
};

let apply = (path, apply0, apply1, apply2) =>
  Response.(
    Numeric.(
      Filter.(
        switch path {
        | [] => apply0()
        | ["sum"] => apply1(sum)
        | ["count"] => apply1(count)
        | ["min"] => apply1(min)
        | ["max"] => apply1(max)
        | ["mean"] => apply1(mean)
        | ["median"] => apply1(median)
        | ["sd"] => apply1(sd)
        | ["filter", s1, "equals", s2] => apply1(equals(s1, s2))
        | ["filter", s1, "contains", s2] => apply1(contains(s1, s2))
        | ["filter", s1, "equals", s2, "sum"] => apply2(equals(s1, s2), sum)
        | ["filter", s1, "contains", s2, "sum"] =>
          apply2(contains(s1, s2), sum)
        | ["filter", s1, "equals", s2, "count"] =>
          apply2(equals(s1, s2), count)
        | ["filter", s1, "contains", s2, "count"] =>
          apply2(contains(s1, s2), count)
        | ["filter", s1, "equals", s2, "min"] => apply2(equals(s1, s2), min)
        | ["filter", s1, "contains", s2, "min"] =>
          apply2(contains(s1, s2), min)
        | ["filter", s1, "equals", s2, "max"] => apply2(equals(s1, s2), max)
        | ["filter", s1, "contains", s2, "max"] =>
          apply2(contains(s1, s2), max)
        | ["filter", s1, "equals", s2, "mean"] => apply2(equals(s1, s2), mean)
        | ["filter", s1, "contains", s2, "mean"] =>
          apply2(contains(s1, s2), mean)
        | ["filter", s1, "equals", s2, "median"] =>
          apply2(equals(s1, s2), median)
        | ["filter", s1, "contains", s2, "median"] =>
          apply2(contains(s1, s2), median)
        | ["filter", s1, "equals", s2, "sd"] => apply2(equals(s1, s2), sd)
        | ["filter", s1, "contains", s2, "sd"] => apply2(contains(s1, s2), sd)
        | _ => Empty
        }
      )
    )
  );

let handle_get_read_ts = (ctx, prov) => {
  open List;
  open Response;
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  let info = Prov.info(prov, "READ");
  let id_list = get_id_list(uri_path);
  switch path_list {
  | ["", "ts", "blob", id, "length"] =>
    Json(Blob_timeseries.length(~ctx=ctx.blobts_ctx, ~info, ~id_list))
  | ["", "ts", id, "length"] =>
    Json(Numeric_timeseries.length(~ctx=ctx.numts_ctx, ~info, ~id_list))
  | ["", "ts", "blob", id, "latest"] =>
    Json(Blob_timeseries.read_latest(~ctx=ctx.blobts_ctx, ~info, ~id_list))
  | ["", "ts", id, "latest", ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        Numeric_timeseries.read_latest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        Numeric_timeseries.read_latest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        Numeric_timeseries.read_latest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | ["", "ts", "blob", id, "earliest"] =>
    Json(Blob_timeseries.read_earliest(~ctx=ctx.blobts_ctx, ~info, ~id_list))
  | ["", "ts", id, "earliest", ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        Numeric_timeseries.read_earliest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        Numeric_timeseries.read_earliest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        Numeric_timeseries.read_earliest(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | ["", "ts", "blob", id, "last", n] =>
    Json(
      Blob_timeseries.read_last(
        ~ctx=ctx.blobts_ctx,
        ~info,
        ~id_list,
        ~n=int_of_string(n)
      )
    )
  | ["", "ts", id, "last", n, ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        read_last(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        read_last(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        read_last(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | ["", "ts", "blob", id, "first", n] =>
    Json(
      Blob_timeseries.read_first(
        ~ctx=ctx.blobts_ctx,
        ~info,
        ~id_list,
        ~n=int_of_string(n)
      )
    )
  | ["", "ts", id, "first", n, ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        read_first(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        read_first(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        read_first(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~n=int_of_string(n),
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | ["", "ts", "blob", id, "since", t] =>
    Json(
      Blob_timeseries.read_since(
        ~ctx=ctx.blobts_ctx,
        ~info,
        ~id_list,
        ~from=int_of_string(t)
      )
    )
  | ["", "ts", id, "since", t, ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        read_since(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t),
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        read_since(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t),
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        read_since(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t),
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | ["", "ts", "blob", id, "range", t1, t2] =>
    Json(
      Blob_timeseries.read_range(
        ~ctx=ctx.blobts_ctx,
        ~info,
        ~id_list,
        ~from=int_of_string(t1),
        ~to_=int_of_string(t2)
      )
    )
  | ["", "ts", id, "range", t1, t2, ...func] =>
    open Numeric_timeseries;
    let apply0 = () =>
      Json(
        read_range(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t1),
          ~to_=int_of_string(t2),
          ~fn=[]
        )
      );
    let apply1 = f =>
      Json(
        read_range(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t1),
          ~to_=int_of_string(t2),
          ~fn=[f]
        )
      );
    let apply2 = (f1, f2) =>
      Json(
        read_range(
          ~ctx=ctx.numts_ctx,
          ~info,
          ~id_list,
          ~from=int_of_string(t1),
          ~to_=int_of_string(t2),
          ~fn=[f1, f2]
        )
      );
    apply(func, apply0, apply1, apply2);
  | _ => Empty
  };
};

let get_id_key = (mode, uri_path) => {
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", mode, id, key] => Some((id, key))
  | _ => None
  };
};

let get_mode = uri_path => Str.first_chars(uri_path, 4);

let handle_get_read_kv_json = (uri_path, ctx, prov) => {
  open Response;
  open Keyvalue.Json;
  let info = Prov.info(prov, "READ");
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "kv", id, "keys"] => Json(keys(~ctx=ctx.jsonkv_ctx, ~info, ~id))
  | ["", "kv", id, "count"] => Json(count(~ctx=ctx.jsonkv_ctx, ~info, ~id))
  | ["", "kv", id, key] => Json(read(~ctx=ctx.jsonkv_ctx, ~info, ~id, ~key))
  | _ => Empty
  };
};

let handle_get_read_kv_text = (uri_path, ctx, prov) => {
  open Response;
  open Keyvalue.Text;
  let info = Prov.info(prov, "READ");
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "kv", id, "keys"] => Json(keys(~ctx=ctx.textkv_ctx, ~info, ~id))
  | ["", "kv", id, "count"] => Json(count(~ctx=ctx.textkv_ctx, ~info, ~id))
  | ["", "kv", id, key] => Text(read(~ctx=ctx.textkv_ctx, ~info, ~id, ~key))
  | _ => Empty
  };
};

let handle_get_read_kv_binary = (uri_path, ctx, prov) => {
  open Response;
  open Keyvalue.Binary;
  let info = Prov.info(prov, "READ");
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "kv", id, "keys"] => Json(keys(~ctx=ctx.binarykv_ctx, ~info, ~id))
  | ["", "kv", id, "count"] => Json(count(~ctx=ctx.binarykv_ctx, ~info, ~id))
  | ["", "kv", id, key] => Text(read(~ctx=ctx.binarykv_ctx, ~info, ~id, ~key))
  | _ => Empty
  };
};

let handle_read_database = (ctx, prov) => {
  open Ack;
  open Response;
  let uri_path = Prov.uri_path(prov);
  let content_format = Prov.content_format(prov);
  let mode = get_mode(uri_path);
  let result =
    switch (mode, content_format) {
    | ("/ts/", 50) => handle_get_read_ts(ctx, prov)
    | ("/kv/", 50) => handle_get_read_kv_json(uri_path, ctx, prov)
    | ("/kv/", 0) => handle_get_read_kv_text(uri_path, ctx, prov)
    | ("/kv/", 42) => handle_get_read_kv_binary(uri_path, ctx, prov)
    | _ => Empty
    };
  switch result {
  | Json(json) => json >>= 
      json' => Lwt.return(Payload(content_format, Ezjsonm.to_string(json')))
  | Text(text) => text >>= 
      text' => Lwt.return(Payload(content_format, text'))
  | Binary(binary) => binary >>= 
      binary' => Lwt.return(Payload(content_format, binary'))
  | Empty => Lwt.return(Code(128))
  };
};

let handle_read_hypercat = (ctx, prov) => {
  open Ack;
  let info = Prov.info(prov, "READ");
  Hc.get(~ctx=ctx.hc_ctx, ~info) >>= 
    json => Ezjsonm.to_string(json) |> 
      s => Payload(50, s) |> Lwt.return;
};

let handle_read_uptime = (ctx, prov) => {
  open Ack;
  uptime() >>=
    json => Ezjsonm.to_string(json) |> 
      s => Payload(50, s) |> Lwt.return
};

let ack = kind => {
  open Ack;
  switch kind {
  | Code(n) => Protocol.Zest.create_ack(n)
  | Payload(format, data) => Protocol.Zest.create_ack_payload(format, data)
  | Observe(key, uuid) => Protocol.Zest.create_ack_observe(key, uuid)
  | Notify(key) => Protocol.Zest.create_ack_notification(key)
  } |> Lwt.return
};

let handle_read_notification = (ctx, prov) => {
  open Ack;
  Notify.add(ctx.notify_ctx, Prov.uri_path(prov));
  Notify(router_public_key^) |> Lwt.return;
};

let handle_get_hello = (ctx, prov) => {
  open Ack;
  Payload(0, "hello world!") |> Lwt.return
};

let handle_get_time = (ctx, prov) => {
  open Ack;
  open Unix;
  let months = [|"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"|];
  let gmt = gmtime(time());
  let s = Printf.sprintf("%s %d %d:%d:%d", months[gmt.tm_mon], gmt.tm_mday, gmt.tm_hour, gmt.tm_min, gmt.tm_sec);
  Payload(0, s) |> Lwt.return
};

let handle_get_read = (ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "time"] => handle_get_time(ctx, prov);
  | ["", "hello"] => handle_get_hello(ctx, prov);
  | ["", "uptime"] => handle_read_uptime(ctx, prov)
  | ["", "cat"] => handle_read_hypercat(ctx, prov)
  | ["", "notification", "response", ..._] => handle_read_notification(ctx, prov)
  | _ => handle_read_database(ctx, prov)
  };
};

let to_json = payload => {
  open Ezjsonm;
  let parsed =
    try (Some(from_string(payload))) {
    | Parse_error(_) => None
    };
  parsed;
};

let handle_post_write_ts_numeric = (~timestamp=None, key, payload, ctx, prov) => {
  open Numeric_timeseries;
  let info = Prov.info(prov, "WRITE");
  let json = to_json(payload);
  switch json {
  | Some(value) =>
    if (is_valid(value)) {
      Some(write(~ctx=ctx.numts_ctx, ~info, ~timestamp, ~id=key, ~json=value));
    } else {
      None;
    }
  | None => None
  };
};

let handle_post_write_ts_blob = (~timestamp=None, key, payload, ctx, prov) => {
  open Blob_timeseries;
  let info = Prov.info(prov, "WRITE");
  let json = to_json(payload);
  switch json {
  | Some(value) =>
    Some(write(~ctx=ctx.blobts_ctx, ~info, ~timestamp, ~id=key, ~json=value))
  | None => None
  };
};

let handle_post_write_ts = (payload, ctx, prov) => {
  open List;
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "ts", "blob", key] =>
    handle_post_write_ts_blob(key, payload, ctx, prov)
  | ["", "ts", "blob", key, "at", ts] =>
    handle_post_write_ts_blob(
      ~timestamp=Some(int_of_string(ts)),
      key,
      payload,
      ctx,
      prov
    )
  | ["", "ts", key] => handle_post_write_ts_numeric(key, payload, ctx, prov)
  | ["", "ts", key, "at", ts] =>
    handle_post_write_ts_numeric(
      ~timestamp=Some(int_of_string(ts)),
      key,
      payload,
      ctx,
      prov
    )
  | _ => None
  };
};

let handle_post_write_kv_json = (payload, ctx, prov) => {
  open Keyvalue.Json;
  let uri_path = Prov.uri_path(prov);
  let info = Prov.info(prov, "WRITE");
  switch (get_id_key("kv", uri_path)) {
  | Some((id, key)) =>
    switch (to_json(payload)) {
    | Some(json) => Some(write(~ctx=ctx.jsonkv_ctx, ~info, ~id, ~key, ~json))
    | None => None
    }
  | None => None
  };
};

let handle_post_write_kv_text = (payload, ctx, prov) => {
  open Keyvalue.Text;
  let info = Prov.info(prov, "WRITE");
  let uri_path = Prov.uri_path(prov);
  switch (get_id_key("kv", uri_path)) {
  | Some((id, key)) => Some(write(~ctx=ctx.textkv_ctx, ~info, ~id, ~key, ~text=payload))
  | None => None
  };
};

let handle_post_write_kv_binary = (payload, ctx, prov) => {
  open Keyvalue.Binary;
  let info = Prov.info(prov, "WRITE");
  let uri_path = Prov.uri_path(prov);
  switch (get_id_key("kv", uri_path)) {
  | Some((id, key)) => Some(write(~ctx=ctx.binarykv_ctx, ~info, ~id, ~key, ~binary=payload))
  | None => None
  };
};

let handle_write_database = (payload, ctx, prov) => {
  open Ack;
  open Ezjsonm;
  let uri_path = Prov.uri_path(prov);
  let content_format = Prov.content_format(prov);
  let mode = get_mode(uri_path);
  let result =
    switch (mode, content_format) {
    | ("/ts/", 50) => handle_post_write_ts(payload, ctx, prov)
    | ("/kv/", 50) => handle_post_write_kv_json(payload, ctx, prov)
    | ("/kv/", 0) => handle_post_write_kv_text(payload, ctx, prov)
    | ("/kv/", 42) => handle_post_write_kv_binary(payload, ctx, prov)
    | _ => None
    };
  switch result {
  | Some(promise) => promise >>= () => Lwt.return(Code(65))
  | None => Lwt.return(Code(128))
  };
};

let handle_write_hypercat = (payload, ctx, prov) => {
  open Ack;
  let info = Prov.info(prov, "WRITE");
  let json = to_json(payload);
  switch json {
  | Some(json) => Hc.update(~ctx=ctx.hc_ctx, ~info, ~item=json) >>=
      result => switch result {
                | Ok => Code(65)
                | Error(n) => Code(n)
                } |> Lwt.return
  | None => Lwt.return(Code(128))
  };
};

let handle_write_notification_request = (payload, ctx, prov) => {
  if (Observe.is_observed(ctx.observe_ctx, Prov.ident(prov))) {
    let ident = Prov.uri_path(prov);
    let payload' = create_data_payload_worker(Some(prov), payload);
    Protocol.Zest.route(ctx.zmq_ctx, ident, payload') >>= 
      () => Ack.Code(65) |> Lwt.return;
  } else {
    Ack.Code(163) |> Lwt.return;
  };
};

let handle_write_notification_response = (payload, ctx, prov) => {
  let ident = Prov.uri_path(prov);
  let payload' = create_data_payload_worker(Some(prov), payload);
  Protocol.Zest.route(ctx.zmq_ctx, ident, payload') >>= 
    () => Ack.Code(65) |> Lwt.return;
};


let handle_post_write = (payload, ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  switch path_list {
  | ["", "cat"] => handle_write_hypercat(payload, ctx, prov)
  | ["", "notification", "request", ..._] => handle_write_notification_request(payload, ctx, prov)
  | ["", "notification", "response", ..._] => handle_write_notification_response(payload, ctx, prov)
  | _ => handle_write_database(payload, ctx, prov)
  };
};

let create_uuid = () =>
  Uuidm.v4_gen(Random.State.make_self_init(), ()) |> Uuidm.to_string;

let is_valid_token = (token, path, meth) =>
  switch token_secret_key^ {
  | "" => true
  | _ =>
    Token.is_valid(
      token,
      token_secret_key^,
      ["path = " ++ path, "method = " ++ meth, "target = " ++ identity^]
    )
  };

let handle_options = (oc, bits) => {
  let options = Array.make(oc, (0, ""));
  let rec handle = (oc, bits) =>
    if (oc == 0) {
      bits;
    } else {
      let (number, value, r) = Protocol.Zest.handle_option(bits);
      let _ = Logger.debug_f("handle_options", Printf.sprintf("%d:%s", number, value));
      options[oc - 1] = (number, value);
      handle(oc - 1, r);
    };
  (options, handle(oc, bits));
};

let handle_get_observed = (ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let token = Prov.token(prov);
  if (is_valid_token(token, uri_path, "GET")) {
    handle_get_read(ctx, prov) >>= 
      resp => route(resp, "", ctx, prov) 
        >>= () => ack(resp);
  } else {
    route(Ack.Code(129), "", ctx, prov) >>= 
      () => ack(Ack.Code(129));
  };
};

let handle_get_unobserved = (ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let token = Prov.token(prov);
  if (is_valid_token(token, uri_path, "GET")) {
    handle_get_read(ctx, prov) >>= ack;
  } else {
    ack(Ack.Code(129));
  };
};

let handle_get_observation_request = (ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let token = Prov.token(prov);
  if (is_valid_token(token, uri_path, "GET")) {
    let uuid = create_uuid();
    Observe.add(ctx.observe_ctx, uuid, prov) >>= 
      () => route(Ack.Observe(router_public_key^, uuid), "", ctx, prov) >>= 
        () => ack(Ack.Observe(router_public_key^, uuid))
  } else {
    route(Ack.Code(129), "", ctx, prov) >>= 
      () => ack(Ack.Code(129));
  };
};

let handle_get = (ctx, prov) => {
  let key = Prov.ident(prov);
  let observed = Prov.observed(prov);
  if (observed == "data" || observed == "audit" || observed == "notification") {
    handle_get_observation_request(ctx, prov);
  } else if (Observe.is_observed(ctx.observe_ctx, key)) {
    handle_get_observed(ctx, prov);
  } else {
    handle_get_unobserved(ctx, prov);
  };
};

let handle_post_unobserved = (payload, ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let token = Prov.token(prov);
  if (is_valid_token(token, uri_path, "POST")) {
    handle_post_write(payload, ctx, prov) >>= ack;
  } else {
    ack(Code(129));
  };
};

let handle_post_observed = (payload, ctx, prov) => {
  let uri_path = Prov.uri_path(prov);
  let token = Prov.token(prov);
  if (is_valid_token(token, uri_path, "POST")) {
    handle_post_write(payload, ctx, prov) >>= 
      resp => route(resp, payload, ctx, prov) >>= 
        () => ack(resp);
  } else {
    route(Ack.Code(129), payload, ctx, prov) >>= 
      () => ack(Code(129));
  };
};

let handle_post = (ctx, payload, prov) => {
  let key = Prov.ident(prov);
  if (Observe.is_observed(ctx.observe_ctx, key)) {
    handle_post_observed(payload, ctx, prov);
  } else {
    handle_post_unobserved(payload, ctx, prov);
  };
};

let handle_delete_write_kv_json = (ctx, prov) => {
  open Keyvalue.Json;
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  let info = Prov.info(prov, "DELETE");
  switch path_list {
  | ["", mode, id, key] => Some(delete(~ctx=ctx.jsonkv_ctx, ~info, ~id, ~key))
  | ["", mode, id] => Some(delete_all(~ctx=ctx.jsonkv_ctx, ~info, ~id))
  | _ => None
  };
};

let handle_delete_write_kv_text = (ctx, prov) => {
  open Keyvalue.Text;
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  let info = Prov.info(prov, "DELETE");
  switch path_list {
  | ["", mode, id, key] => Some(delete(~ctx=ctx.textkv_ctx, ~info, ~id, ~key))
  | ["", mode, id] => Some(delete_all(~ctx=ctx.textkv_ctx, ~info, ~id))
  | _ => None
  };
};

let handle_delete_write_kv_binary = (ctx, prov) => {
  open Keyvalue.Binary;
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  let info = Prov.info(prov, "DELETE");
  switch path_list {
  | ["", mode, id, key] =>
    Some(delete(~ctx=ctx.binarykv_ctx, ~info, ~id, ~key))
  | ["", mode, id] => Some(delete_all(~ctx=ctx.binarykv_ctx, ~info, ~id))
  | _ => None
  };
};

let has_unsupported_delete_api = lis =>
  switch lis {
  | [] => false
  | ["", "ts", "blob", _, "first", ..._] => true
  | ["", "ts", "blob", _, "last", ..._] => true
  | ["", "ts", _, "first", ..._] => true
  | ["", "ts", _, "last", ..._] => true
  | _ =>
    switch (List.rev(lis)) {
    | [x, ..._] when x == "sum" => true
    | [x, ..._] when x == "count" => true
    | [x, ..._] when x == "min" => true
    | [x, ..._] when x == "max" => true
    | [x, ..._] when x == "mean" => true
    | [x, ..._] when x == "median" => true
    | [x, ..._] when x == "sd" => true
    | [x, ..._] when x == "length" => true
    | _ => false
    }
  };

let handle_delete_ts_numeric = (ctx, prov) => {
  open Numeric_timeseries;
  let uri_path = Prov.uri_path(prov);
  let info = Prov.info(prov, "DELETE");
  switch (handle_get_read_ts(ctx, prov)) {
  | Json(json) =>
    Some(delete(~ctx=ctx.numts_ctx, ~info, ~id_list=get_id_list(uri_path), ~json))
  | _ => None
  };
};

let handle_delete_ts_blob = (ctx, prov) => {
  open Blob_timeseries;
  let uri_path = Prov.uri_path(prov);
  let info = Prov.info(prov, "DELETE");
  switch (handle_get_read_ts(ctx, prov)) {
  | Json(json) =>
    Some(delete(~ctx=ctx.blobts_ctx, ~info, ~id_list=get_id_list(uri_path), ~json))
  | _ => None
  };
};

let handle_delete_write = (ctx, prov) => {
  open Ack;
  open Ezjsonm;
  let content_format = Prov.content_format(prov);
  let uri_path = Prov.uri_path(prov);
  let path_list = String.split_on_char('/', uri_path);
  if (has_unsupported_delete_api(path_list)) {
    Lwt.return(Code(134));
  } else {
    let result =
      switch (path_list, content_format) {
      | (["", "kv", ..._], 50) => handle_delete_write_kv_json(ctx, prov)
      | (["", "kv", ..._], 0) => handle_delete_write_kv_text(ctx, prov)
      | (["", "kv", ..._], 42) => handle_delete_write_kv_binary(ctx, prov)
      | (["", "ts", "blob", ..._], 50) => handle_delete_ts_blob(ctx, prov)
      | (["", "ts", ..._], 50) => handle_delete_ts_numeric(ctx, prov)
      | _ => None
      };
    switch result {
    | Some(promise) => promise >>= () => Lwt.return(Code(66))
    | None => Lwt.return(Code(128))
    };
  };
};

let handle_delete_observed = (ctx, prov) => {
  let token = Prov.token(prov);
  let uri_path = Prov.uri_path(prov);
  if (is_valid_token(token, uri_path, "DELETE")) {
    handle_delete_write(ctx, prov) >>= 
      resp => route(resp, "", ctx, prov) >>= 
        () => ack(resp);
  } else {
    route(Ack.Code(129), "", ctx, prov) >>= 
      () => ack(Code(129));
  };
};

let handle_delete_unobserved = (ctx, prov) => {
  let token = Prov.token(prov);
  let uri_path = Prov.uri_path(prov);
  if (is_valid_token(token, uri_path, "DELETE")) {
    handle_delete_write(ctx, prov) >>= ack;
  } else {
    ack(Code(129));
  };
};

let handle_delete = (ctx, prov) => {
  let key = Prov.ident(prov);
  if (Observe.is_observed(ctx.observe_ctx, key)) {
    handle_delete_observed(ctx, prov);
  } else {
    handle_delete_unobserved(ctx, prov);
  };
};

let handle_msg = (msg, ctx) => {
  open Logger;
  handle_expire(ctx) >>= 
    () => debug_f("handle_msg", Printf.sprintf("Received:\n%s", to_hex(msg))) >>= 
      () => {
        let r0 = Bitstring.bitstring_of_string(msg);
        let (tkl, oc, code, r1) = Protocol.Zest.handle_header(r0);
        let (token, r2) = Protocol.Zest.handle_token(r1, tkl);
        let (options, r3) = handle_options(oc, r2);
        let payload = Bitstring.string_of_bitstring(r3);
        let prov = Prov.create(~code, ~options, ~token);
        switch code {
        | 1 => handle_get(ctx, prov)
        | 2 => handle_post(ctx, payload, prov)
        | 4 => handle_delete(ctx, prov)
        | _ => failwith("invalid code")
        };
      }  
};

let server = ctx => {
  open Logger;
  let rec loop = () =>
    Protocol.Zest.recv(ctx.zmq_ctx) >>= 
      msg => handle_msg(msg, ctx) >>=
        resp => Protocol.Zest.send(ctx.zmq_ctx, resp) >>=
          () => Logger.debug_f("server", Printf.sprintf("Sending:\n%s", to_hex(resp))) >>= 
            () => loop();
  Logger.info_f("server", "active") >>= () => loop();
};

/* test key: uf4XGHI7[fLoe&aG1tU83[ptpezyQMVIHh)J=zB1 */
let parse_cmdline = () => {
  let usage = "usage: " ++ Sys.argv[0] ++ " [--debug] [--secret-key string]";
  let speclist = [
    (
      "--request-endpoint",
      Arg.Set_string(rep_endpoint),
      ": to set the request/reply endpoint"
    ),
    (
      "--router-endpoint",
      Arg.Set_string(rout_endpoint),
      ": to set the router/dealer endpoint"
    ),
    ("--enable-logging", Arg.Set(log_mode), ": turn debug mode on"),
    (
      "--secret-key-file",
      Arg.Set_string(server_secret_key_file),
      ": to set the curve secret key"
    ),
    (
      "--token-key-file",
      Arg.Set_string(token_secret_key_file),
      ": to set the token secret key"
    ),
    ("--identity", Arg.Set_string(identity), ": to set the server identity"),
    (
      "--store-dir",
      Arg.Set_string(store_directory),
      ": to set the location for the database files"
    )
  ];
  Arg.parse(speclist, x => raise(Arg.Bad("Bad argument : " ++ x)), usage);
};

let setup_router_keys = () => {
  let (public_key, private_key) = ZMQ.Curve.keypair();
  router_secret_key := private_key;
  router_public_key := public_key;
};

let data_from_file = file => {
  Fpath.v(file) |> 
    Bos.OS.File.read |> 
      result =>switch result {
      | Rresult.Error(_) => failwith("failed to access file")
      | Rresult.Ok(key) => key
      }
};

let set_server_key = file => server_secret_key := data_from_file(file);

let set_token_key = file => {
  if (file != "") {
    token_secret_key := data_from_file(file);
  }
};

let cleanup_observation = ctx => {
  Observe.get_all(ctx.observe_ctx) |> 
    uuids => route_message(uuids, ctx, Ack.Code(163), Protocol.Zest.create_ack(163), None) >>= 
      () => Lwt_unix.sleep(1.0);
};

let cleanup_notification = ctx => {
  let idents = Notify.get_all(ctx.notify_ctx);
  let payload = Protocol.Zest.create_ack(163);
  Lwt_list.iter_s(ident => Protocol.Zest.route(ctx.zmq_ctx, ident, payload), idents);
};

let terminate_server = (ctx, m) => {
  let info = Printf.sprintf("event = TERMINATE, trigger = (%s)", m);
  Lwt_io.printf("\nShutting down server...\n") >>= 
    () => Blob_timeseries.flush(~ctx=ctx.blobts_ctx, ~info) >>= 
      () => Numeric_timeseries.flush(~ctx=ctx.numts_ctx, ~info) >>= 
        () => cleanup_observation(ctx) >>=
          () => cleanup_notification(ctx) >>=
            () => Protocol.Zest.close(ctx.zmq_ctx) |> 
              () => exit(0);
};

let unhandled_error = (e, ctx) => {
  let msg = Printexc.to_string(e);
  let stack = Printexc.get_backtrace();
  Logger.error_f("unhandled_error", Printf.sprintf("%s%s", msg, stack)) >>= 
    () => ack(Ack.Code(160)) >>= resp => Protocol.Zest.send(ctx.zmq_ctx, resp);
};

exception Interrupt(string);

let register_signal_handlers = () => {
  Lwt_unix.(on_signal(Sys.sigterm, (_) => raise(Interrupt("Caught SIGTERM"))) |> 
    id => on_signal(Sys.sighup, (_) => raise(Interrupt("Caught SIGHUP"))) |> 
      id => on_signal(Sys.sigint, (_) => raise(Interrupt("Caught SIGINT"))));
};

let rec run_server = ctx => {
  let _ =
    try (Lwt_main.run(server(ctx))) {
    | Interrupt(m) => terminate_server(ctx, m)
    | ZMQ.ZMQ_exception(e, m) =>
      ack(Ack.Code(163)) >>= Protocol.Zest.send(ctx.zmq_ctx)
    | Stack_overflow => ack(Ack.Code(141)) >>= Protocol.Zest.send(ctx.zmq_ctx)
    | e => unhandled_error(e, ctx)
    };
  run_server(ctx);
};

let init = () => {
  let jsonkv_ctx = Keyvalue.Json.create(~path_to_db=store_directory^);
  {
    hc_ctx: Hc.create(~store=jsonkv_ctx),
    observe_ctx: Observe.create(),
    notify_ctx: Notify.create(),
    numts_ctx: Numeric_timeseries.create(~path_to_db=store_directory^, ~max_buffer_size=10000, ~shard_size=1000),
    blobts_ctx: Blob_timeseries.create(~path_to_db=store_directory^, ~max_buffer_size=1000, ~shard_size=100),
    jsonkv_ctx: jsonkv_ctx,
    textkv_ctx: Keyvalue.Text.create(~path_to_db=store_directory^),
    binarykv_ctx: Keyvalue.Binary.create(~path_to_db=store_directory^),
    zmq_ctx: Protocol.Zest.create(~endpoints=(rep_endpoint^, rout_endpoint^), ~keys=(server_secret_key^, router_secret_key^)),
    version: 1
    };
};

let setup_server = () => {
  parse_cmdline();
  log_mode^ ? Logger.init() : ();
  setup_router_keys();
  set_server_key(server_secret_key_file^);
  set_token_key(token_secret_key_file^);
  let ctx = init();
  let _ = register_signal_handlers();
  run_server(ctx) |> (() => terminate_server(ctx));
};

setup_server();
