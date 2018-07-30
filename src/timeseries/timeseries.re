open Lwt.Infix;

type t = {
  membuf: Membuf.t,
  index: Lwt.t(Index.t),
  shard: Lwt.t(Shard.t),
  max_buffer_size: int,
  shard_size: int
};

let create = (~path_to_db as path, ~max_buffer_size as mbs, ~shard_size as ss) => {
  membuf: Membuf.create(),
  index: Index.create(path ++ "_index_store"),
  shard: Shard.create(path ++ "_shard_store"),
  max_buffer_size: mbs,
  shard_size: ss
};

let shard_range = (lis) => {
  open List;
  let cmp = (x, y) => x > y ? 1 : (-1);
  switch lis {
  | [] => None
  | _ => Some(map(((ts, _)) => ts, lis) |> sort(cmp) |> ((lis') => (hd(lis'), hd(rev(lis')))))
  };
};

let make_key = (id, (t1, t2)) => [id, string_of_int(t1), string_of_int(t2)];

let shard_data = (ctx, id) => {
  let rec loop = (n, shard) =>
    if (n > 0) {
      Membuf.read(ctx.membuf, id) >>= ((elt) => loop(n - 1, List.cons(elt, shard)));
    } else {
      Lwt.return(shard);
    };
  loop(ctx.shard_size, []);
};

let get_time = () => {
  let t_sec = Unix.gettimeofday();
  let t_ms = t_sec *. 1000.0;
  int_of_float(t_ms);
};

let make_elt = (timestamp, json) =>
  switch timestamp {
  | Some((t)) => (t, json)
  | None => (get_time(), json)
  };

let string_of_key = (lis) => List.fold_left((x, y) => x ++ ":" ++ y, "", lis);

let log_index = (str, lis) =>
  Lwt_list.iter_s(((x, y)) => Lwt_io.printf("%s:(%d,%d)\n", str, x, y), lis);

let remove_leftover_shards = (ctx, k, keep_index, remove_list, info) => {
  open List;
  let index_list = filter((i) => i != keep_index, remove_list);
  let key_list = map((i) => make_key(k, i), index_list);
  Shard.remove(ctx.shard, info, key_list);
};

let handle_shard_overlap_worker = (ctx, k, shard, shard_lis, overlap_list, info) => {
  open List;
  let new_shard = flatten(cons(shard, shard_lis));
  Lwt_log_core.debug_f("shard len:%d", List.length(new_shard))
  >>= (
    () =>
      switch (shard_range(new_shard)) {
      | Some((new_range)) =>
        let key = make_key(k, new_range);
        Lwt_log_core.debug_f("Adding shard with key:%s", string_of_key(key))
        >>= (
          () =>
            Index.update(ctx.index, info, k, new_range, overlap_list)
            >>= (
              (bounds) =>
                Membuf.set_disk_range(ctx.membuf, k, bounds)
                |> (
                  () =>
                    Shard.add(ctx.shard, info, key, new_shard)
                    >>= (() => remove_leftover_shards(ctx, k, new_range, overlap_list, info))
                )
            )
        );
      | None => Lwt.return_unit
      }
  );
};

let handle_shard_overlap = (ctx, k, shard, range, info) =>
  Index.overlap(ctx.index, k, range)
  >>= (
    (overlap_list) =>
      log_index("=== overlapping", overlap_list)
      >>= (
        () =>
          Lwt_list.map_s((r) => Shard.get(ctx.shard, make_key(k, r)), overlap_list)
          >>= (
            (shard_list) =>
              handle_shard_overlap_worker(ctx, k, shard, shard_list, overlap_list, info)
          )
      )
  );

let handle_shard = (ctx, k, shard, info) =>
  switch (shard_range(shard)) {
  | Some((range)) => handle_shard_overlap(ctx, k, shard, range, info)
  | None => Lwt.return_unit
  };

let is_ascending = (ctx, k) =>
  Membuf.get_disk_range(ctx.membuf, k)
  |> (
    (range) =>
      switch range {
      | None => false
      | Some(((lb, ub))) => Membuf.is_ascending(ctx.membuf, k, ub)
      }
  );

let is_descending = (ctx, k) =>
  Membuf.get_disk_range(ctx.membuf, k)
  |> (
    (range) =>
      switch range {
      | None => false
      | Some(((lb, ub))) => Membuf.is_descending(ctx.membuf, k, lb)
      }
  );

let write = (~ctx, ~info, ~timestamp as ts=None, ~id as k, ~json as v) => {
  let (t, j) = make_elt(ts, v);
  Membuf.write(ctx.membuf, k, (t, j))
  >>= (
    () =>
      Membuf.length(ctx.membuf, k)
      >>= (
        (current_buffer_size) =>
          if (current_buffer_size == ctx.max_buffer_size) {
            shard_data(ctx, k) >>= ((shard) => handle_shard(ctx, k, shard, info));
          } else {
            Lwt.return_unit;
          }
      )
  );
};

let flush_series = (ctx, k, shard, info) =>
  handle_shard(ctx, k, shard, info) >>= (() => Membuf.empty_series(ctx.membuf, k));

let flush = (~ctx, ~info) =>
  Membuf.serialise(ctx.membuf)
  >>= ((lis) => Lwt_list.iter_s(((key, shard)) => flush_series(ctx, key, shard, info), lis));

let take = (n, lis) => {
  open List;
  let rec loop = (n, acc, l) =>
    switch l {
    | [] => acc
    | [xs, ...rest] when n == 0 => acc
    | [xs, ...rest] => loop(n - 1, cons(xs, acc), rest)
    };
  rev(loop(n, [], lis));
};

let sort_result = (mode, lis) =>
  List.(
    switch mode {
    | `Last => fast_sort(((x, y), (x', y')) => x < x' ? 1 : (-1), lis)
    | `First => fast_sort(((x, y), (x', y')) => x > x' ? 1 : (-1), lis)
    | `None => lis
    }
  );

let take_from_memory = (n, lis, mode) => {
  open List;
  let count = min(n, length(lis));
  let sorted = sort_result(mode, lis);
  (n - count, take(count, sorted)) |> Lwt.return;
};

let read_memory = (ctx, id, n, mode) =>
  Lwt_io.printf("read_memory\n")
  >>= (
    () => Membuf.to_list(ctx.membuf, id) >>= ((mem_shard) => take_from_memory(n, mem_shard, mode))
  );

let read_memory_all = (ctx, id) =>
  Membuf.exists(ctx.membuf, id) ? Membuf.to_list(ctx.membuf, id) : Lwt.return([]);

let log_shard = (shard) =>
  List.map(((_, json)) => json, shard)
  |> ((lis) => List.iter((elt) => Printf.printf("%s\n", Ezjsonm.to_string(elt)), lis));

let read_disk = (ctx, k, n, mode) => {
  open List;
  let rec loop = (n, acc, lis) =>
    switch lis {
    | [] => acc |> Lwt.return
    | [tup, ...rest] =>
      Shard.get(ctx.shard, make_key(k, tup))
      >>= (
        (shard) => {
          let leftover = n - length(shard);
          if (leftover > 0) {
            loop(leftover, rev_append(shard, acc), rest);
          } else {
            rev_append(take(n, sort_result(mode, shard)), acc) |> Lwt.return;
          };
        }
      )
    };
  Lwt_io.printf("read_disk\n")
  >>= (
    () =>
      Index.get(ctx.index, k)
      >>= (
        (data) =>
          (
            switch (mode, data) {
            | (`Last, Some((lis))) => lis
            | (`First, Some((lis))) => rev(lis)
            | (_, None) => []
            }
          )
          |> loop(n, [])
      )
  );
};

let to_json = (lis) =>
  Ezjsonm.(
    List.(
      rev_map(((t, json)) => dict([("timestamp", int(t)), ("data", value(json))]), lis)
      |> rev
      |> ((lis') => `A(lis'))
    )
  );

let return_data = (~sort as mode, lis) => sort_result(mode, lis) |> to_json |> Lwt.return;

let flush_memory = (ctx, k, info) =>
  read_memory_all(ctx, k) >>= ((shard) => flush_series(ctx, k, shard, info));

let flush_memory_read_from_disk = (ctx, k, n, mode, info) =>
  Lwt_io.printf("flush_memory_read_from_disk\n")
  >>= (() => flush_memory(ctx, k, info) >>= (() => read_disk(ctx, k, n, mode)));

let read_memory_then_disk = (ctx, k, n, mode) =>
  Lwt_io.printf("read_memory_then_disk\n")
  >>= (
    () =>
      read_memory(ctx, k, n, mode)
      >>= (
        ((leftover, mem)) =>
          if (leftover > 0) {
            read_disk(ctx, k, leftover, mode)
            >>= ((disk) => List.rev_append(mem, disk) |> Lwt.return);
          } else {
            mem |> Lwt.return;
          }
      )
  );

let read_last_worker = (~ctx, ~id as k, ~n, ~info) =>
  if (Membuf.exists(ctx.membuf, k)) {
    is_ascending(ctx, k) ?
      read_memory_then_disk(ctx, k, n, `Last) : flush_memory_read_from_disk(ctx, k, n, `Last, info);
  } else {
    read_disk(ctx, k, n, `Last);
  };

let read_last = (~ctx, ~info, ~id_list, ~n) =>
  Lwt_list.fold_left_s(
    (acc, id) =>
      read_last_worker(~ctx=ctx, ~id=id, ~n=n, ~info=info)
      >>= ((x) => List.rev_append(x, acc) |> Lwt.return),
    [],
    id_list
  )
  >>= ((result) => return_data(~sort=`Last, result));

let read_latest = (~ctx, ~info, ~id_list) =>
  read_last(~ctx=ctx, ~info=info, ~id_list=id_list, ~n=1);

let read_first_worker = (~ctx, ~id as k, ~n, ~info) =>
  if (Membuf.exists(ctx.membuf, k)) {
    is_descending(ctx, k) ?
      read_memory_then_disk(ctx, k, n, `First) :
      flush_memory_read_from_disk(ctx, k, n, `First, info);
  } else {
    read_disk(ctx, k, n, `First);
  };

let read_first = (~ctx, ~info, ~id_list, ~n) =>
  Lwt_list.fold_left_s(
    (acc, id) =>
      read_first_worker(~ctx=ctx, ~id=id, ~n=n, ~info=info)
      >>= ((x) => List.rev_append(x, acc) |> Lwt.return),
    [],
    id_list
  )
  >>= ((result) => return_data(~sort=`First, result));

let read_earliest = (~ctx, ~info, ~id_list) =>
  read_first(~ctx=ctx, ~info=info, ~id_list=id_list, ~n=1);

let number_of_records_on_disk = (ctx, k, lis) =>
  Lwt_list.fold_left_s(
    (acc, x) =>
      Shard.get(ctx.shard, make_key(k, x)) >>= ((x) => List.length(x) + acc |> Lwt.return),
    0,
    lis
  );

let number_of_records_in_memory = (ctx, k) =>
  Membuf.(exists(ctx.membuf, k) ? length(ctx.membuf, k) : Lwt.return(0));

let length_of_json = (result) => Ezjsonm.(dict([("length", int(result))]) |> Lwt.return);

let length_worker = (~ctx, ~id as k) =>
  Ezjsonm.(
    Index.get(ctx.index, k)
    >>= (
      (data) =>
        (
          switch data {
          | Some((lis)) => number_of_records_on_disk(ctx, k, lis)
          | None => 0 |> Lwt.return
          }
        )
        >>= ((disk) => number_of_records_in_memory(ctx, k) >>= ((mem) => Lwt.return(disk + mem)))
    )
  );

let length = (~ctx, ~info, ~id_list) =>
  Ezjsonm.(
    Lwt_list.fold_left_s(
      (acc, id) => length_worker(~ctx=ctx, ~id=id) >>= ((x) => x + acc |> Lwt.return),
      0,
      id_list
    )
    >>= ((result) => length_of_json(result))
  );

let make_filter_elt = (k, tup, mode) => (mode, make_key(k, tup));

let filter_since = (ts, lis) => List.filter(((t, _)) => t >= ts, lis);

let read_since_disk_worker = (ctx, k, ts, status) =>
  switch status {
  | `Complete => Shard.get(ctx.shard, k)
  | `Partial => Shard.get(ctx.shard, k) >>= ((shard) => filter_since(ts, shard) |> Lwt.return)
  };

let handle_read_since_disk = (ctx, ts, lis) =>
  Lwt_list.fold_left_s(
    (acc, (status, key)) =>
      read_since_disk_worker(ctx, key, ts, status)
      >>= ((x) => List.rev_append(x, acc) |> Lwt.return),
    [],
    lis
  );

let read_since_disk = (ctx, k, ts) => {
  open List;
  let rec loop = (acc, lis) =>
    switch lis {
    | [] => acc
    | [(lb,ub), ...rest] when lb >= ts && ub >= ts =>
      loop(cons(make_filter_elt(k, (lb, ub), `Complete), acc), rest)
    | [(lb,ub), ...rest] when ub >= ts =>
      cons(make_filter_elt(k, (lb, ub), `Partial), acc)
    | [_, ...rest] => loop(acc, rest)
    };
  Index.get(ctx.index, k)
  >>= (
    (data) =>
      (
        switch data {
        | Some((lis)) => lis
        | None => []
        }
      )
      |> loop([])
      |> handle_read_since_disk(ctx, ts)
  );
};

let read_since_memory = (ctx, k, ts) =>
  read_memory_all(ctx, k) >>= ((data) => filter_since(ts, data) |> Lwt.return);

let read_since_worker = (~ctx, ~id as k, ~from as ts) =>
  read_since_memory(ctx, k, ts)
  >>= (
    (mem) => read_since_disk(ctx, k, ts) >>= ((disk) => List.rev_append(mem, disk) |> Lwt.return)
  );

let read_since = (~ctx, ~info, ~id_list, ~from as ts) =>
  Lwt_list.fold_left_s(
    (acc, id) =>
      read_since_worker(~ctx=ctx, ~id=id, ~from=ts)
      >>= ((x) => List.rev_append(x, acc) |> Lwt.return),
    [],
    id_list
  )
  >>= ((result) => return_data(~sort=`Last, result));

let filter_until = (ts, lis) => List.filter(((t, _)) => t <= ts, lis);

let read_range_worker = (~ctx, ~id as k, ~from as t1, ~to_ as t2) =>
  read_since_memory(ctx, k, t1)
  >>= (
    (mem) =>
      read_since_disk(ctx, k, t1)
      >>= ((disk) => List.rev_append(mem, disk) |> filter_until(t2) |> Lwt.return)
  );

let read_range = (~ctx, ~info, ~id_list, ~from as t1, ~to_ as t2) =>
  Lwt_list.fold_left_s(
    (acc, id) =>
      read_range_worker(~ctx=ctx, ~id=id, ~from=t1, ~to_=t2)
      >>= ((x) => List.rev_append(x, acc) |> Lwt.return),
    [],
    id_list
  )
  >>= ((result) => return_data(~sort=`Last, result));

let get_timestamps = (json) =>
  Ezjsonm.(List.rev_map((x) => get_int(x), get_list((x) => find(x, ["timestamp"]), json)));

let filter_shard_worker = (ctx, key, timestamps, info) =>
  Shard.get(ctx.shard, key)
  >>= (
    (lis) =>
      List.filter(((t, _)) => ! List.mem(t, timestamps), lis)
      |> ((lis') => Shard.add(ctx.shard, info, key, lis'))
  );

let delete_worker = (ctx, key_list, timestamps, info) =>
  Lwt_list.iter_s((k) => filter_shard_worker(ctx, k, timestamps, info), key_list);

let flush_memory_worker = (ctx, id, info) =>
  Membuf.exists(ctx.membuf, id) ? flush_memory(ctx, id, info) : Lwt.return_unit;

let make_shard_keys_worker = (id, lb, lis) => {
  let rec loop = (acc, lis) =>
    switch lis {
    | [] => acc
    | [(t1,t2), ...rest] when lb > t2 => loop(acc, rest)
    | [(t1,t2), ...rest] => loop(List.cons(make_key(id, (t1, t2)), acc), rest)
    };
  loop([], lis);
};

let make_shard_keys = (ctx, id, lb) =>
  Index.get(ctx.index, id)
  >>= (
    (lis) =>
      (
        switch lis {
        | None => []
        | Some((lis')) => make_shard_keys_worker(id, lb, lis')
        }
      )
      |> Lwt.return
  );

let delete = (~ctx, ~info, ~id_list, ~json) =>
  json
  >>= (
    (json') => {
      let timestamps = get_timestamps(Ezjsonm.value(json'));
      switch timestamps {
      | [] => Lwt.return_unit
      | [lb, ..._] =>
        Lwt_list.iter_s((id) => flush_memory_worker(ctx, id, info), id_list)
        >>= (
          () =>
            Lwt_list.map_s((k) => make_shard_keys(ctx, k, lb), id_list)
            >>= ((keys') => Lwt_list.iter_s((k) => delete_worker(ctx, k, timestamps, info), keys'))
        )
      };
    }
  );