include Timeseries;

open Lwt.Infix;

let is_valid = (json) =>
  Ezjsonm.(
    switch (get_dict(json)) {
    | [("value",`Float n)] => true
    | [(tag_name, `String tag_value), ("value",`Float n)] => true
    | [("value",`Float n), (tag_name, `String tag_value)] => true
    | _ => false
    }
  );

let create = (~path_to_db as path, ~max_buffer_size as mbs, ~shard_size as ss) =>
  Timeseries.create(~path_to_db=path ++ __MODULE__, ~max_buffer_size=mbs, ~shard_size=ss);

let apply = (fn, data) => List.fold_left((acc, f) => f(acc), data, fn) |> Lwt.return;

let read_last = (~ctx as branch, ~info, ~id_list, ~n, ~fn) =>
  Timeseries.read_last(~ctx=branch, ~info=info, ~id_list=id_list, ~n=n) >>= apply(fn);

let read_latest = (~ctx as branch, ~info, ~id_list, ~fn) =>
  Timeseries.read_latest(~ctx=branch, ~info=info, ~id_list=id_list) >>= apply(fn);

let read_first = (~ctx as branch, ~info, ~id_list, ~n, ~fn) =>
  Timeseries.read_first(~ctx=branch, ~info=info, ~id_list=id_list, ~n=n) >>= apply(fn);

let read_earliest = (~ctx as branch, ~info, ~id_list, ~fn) =>
  Timeseries.read_earliest(~ctx=branch, ~info=info, ~id_list=id_list) >>= apply(fn);

let read_since = (~ctx, ~info, ~id_list, ~from as ts, ~fn) =>
  Timeseries.read_since(~ctx=ctx, ~info=info, ~id_list=id_list, ~from=ts) >>= apply(fn);

let read_range = (~ctx, ~info, ~id_list, ~from as t1, ~to_ as t2, ~fn) =>
  Timeseries.read_range(~ctx=ctx, ~info=info, ~id_list=id_list, ~from=t1, ~to_=t2) >>= apply(fn);