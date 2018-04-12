include Timeseries;

open Lwt.Infix;

let is_valid json => {
  open Ezjsonm;
  switch (get_dict json) {
  | [("value",`Float n)] => true;
  | [(tag_name, `String tag_value), ("value",`Float n)] => true;
  | [("value",`Float n), (tag_name, `String tag_value)] => true;
  | _ => false;
  };
};

let create path_to_db::path max_buffer_size::mbs shard_size::ss => {
  Timeseries.create path_to_db::(path ^ __MODULE__) max_buffer_size::mbs shard_size::ss;
};

let apply fn data => {
  List.fold_left (fun acc f => f acc) data fn |> Lwt.return;
};

let read_last ctx::branch id::id n::n fn::fn => {
  Timeseries.read_last ctx::branch id::id n::n >>= apply fn;
};

let read_latest ctx::branch id::id fn::fn => {
  Timeseries.read_latest ctx::branch id::id >>= apply fn;
};

let read_first ctx::branch id::id n::n fn::fn => {
  Timeseries.read_first ctx::branch id::id n::n >>= apply fn;
};

let read_firsts ctx::branch id_list::id_list n::n fn::fn => {
  Timeseries.read_firsts ctx::branch id_list::id_list n::n >>= apply fn;
};

let read_earliest ctx::branch id::id fn::fn => {
  Timeseries.read_earliest ctx::branch id::id >>= apply fn;
};

let read_since ctx::ctx id::k from::ts fn::fn => {
  Timeseries.read_since ctx::ctx id::k from::ts >>= apply fn;
};

let read_range ctx::ctx id::k from::t1 to::t2 fn::fn => {
  Timeseries.read_range ctx::ctx id::k from::t1 to::t2 >>= apply fn;
};