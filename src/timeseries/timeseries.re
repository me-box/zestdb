open Lwt.Infix;

type t = {
  membuf: Membuf.t,
  index: Lwt.t Index.t,
  shard: Lwt.t Shard.t,
  max_buffer_size: int,
  shard_size: int
};

let create path_to_db::path max_buffer_size::mbs shard_size::ss => {
  {
    membuf: Membuf.create (),
    index: Index.create (path ^ "_index_store"),
    shard: Shard.create (path ^ "_shard_store"),
    max_buffer_size: mbs,
    shard_size: ss
  };
};

let shard_range lis => {
  open List;
  let cmp x y => x > y ? 1 : -1;  
  switch lis {
  | [] => None;
  | _ => Some (map (fun (ts, _) => ts) lis |> 
     sort cmp |> fun lis' => (hd lis', hd (rev lis')));
  };
};

let make_key id (t1, t2) => {
  [id, string_of_int t1, string_of_int t2];
};

let list_of_keys ctx id => {
  Index.get ctx.index id >>= 
    fun lis => switch lis {
    | None => []
    | Some lis' => List.map (fun (t1, t2) => make_key id (t1, t2)) lis';
    } |> Lwt.return;
};

let shard_data ctx id => {
  let rec loop n shard => {
    if (n > 0) {
      Membuf.read ctx.membuf id >>= 
        fun elt =>
          loop (n - 1) (List.cons elt shard);
    } else {
      Lwt.return shard;
    };
  };
  loop ctx.shard_size [];
};

let get_time () => {
  let t_sec = Unix.gettimeofday ();
  let t_ms = t_sec *. 1000.0;
  int_of_float t_ms;
};

let make_elt timestamp json => {
  switch timestamp {
    | Some t => (t, json);
    | None => (get_time (), json);
    };
};


let string_of_key lis => {
  List.fold_left (fun x y => x ^ ":" ^ y) "" lis;  
};

let log_index str lis => {
  Lwt_list.iter_s (fun (x,y) => Lwt_io.printf "%s:(%d,%d)\n" str x y) lis;
};


let remove_leftover_shards ctx k keep_index remove_list => {
  open List; 
  let index_list = filter (fun i => i != keep_index) remove_list;
  let key_list = map (fun i => make_key k i) index_list;
  Shard.remove ctx.shard key_list;
};


let handle_shard_overlap_worker ctx k shard shard_lis overlap_list => {
  open List;    
  let new_shard = flatten (cons shard shard_lis);
  Lwt_log_core.debug_f "shard len:%d" (List.length new_shard) >>= fun () =>
    switch (shard_range new_shard) {
    | Some new_range => {
      let key = make_key k new_range;
      Lwt_log_core.debug_f "Adding shard with key:%s" (string_of_key key) >>= fun () =>
        Index.update ctx.index k new_range overlap_list >>= fun bounds =>
          Membuf.set_disk_range ctx.membuf k bounds |> fun () =>
            Shard.add ctx.shard key new_shard >>= fun () =>
              remove_leftover_shards ctx k new_range overlap_list;
    };
    | None => Lwt.return_unit;
  };
};

let handle_shard_overlap ctx k shard range => {
  Index.overlap ctx.index k range >>= fun overlap_list => {
    log_index "=== overlapping" overlap_list >>= fun () =>
      Lwt_list.map_s (fun r => Shard.get ctx.shard (make_key k r)) overlap_list >>=
        fun shard_list => handle_shard_overlap_worker ctx k shard shard_list overlap_list;
  };
};

let handle_shard ctx k shard => {
  switch (shard_range shard) {
  | Some range => handle_shard_overlap ctx k shard range; 
  | None => Lwt.return_unit;
  };
};


let is_ascending ctx k => {
  Membuf.get_disk_range ctx.membuf k |>
    fun range => switch range {
      | None => false;
      | Some (lb,ub) => Membuf.is_ascending ctx.membuf k ub;
      };
};

let is_descending ctx k => {
  Membuf.get_disk_range ctx.membuf k |> 
    fun range => switch range {
      | None => false;
      | Some (lb,ub) => Membuf.is_descending ctx.membuf k lb;
      };
};

let write ctx::ctx timestamp::ts=None id::k json::v => {
  let (t, j) = make_elt ts v;
  Membuf.write ctx.membuf k (t,j) >>= fun () =>
    Membuf.length ctx.membuf k >>= fun current_buffer_size => {
      if (current_buffer_size == ctx.max_buffer_size) {
        shard_data ctx k >>= fun shard => {
          handle_shard ctx k shard;
        };
      } else {
        Lwt.return_unit;
      };
    };
};


let flush_series ctx k shard => {
  handle_shard ctx k shard >>=
    fun () => Membuf.empty_series ctx.membuf k;
};

let flush ctx::ctx => {
  Membuf.serialise ctx.membuf >>= fun lis =>
    (Lwt_list.iter_s (fun (key, shard) => 
      flush_series ctx key shard) lis);
};

let take n lis => {
  open List;
  let rec loop n acc l =>
    switch l {
    | [] => acc;
    | [xs, ...rest] when n == 0 => acc;
    | [xs, ...rest] => loop (n - 1) (cons xs acc) rest;
    };
  rev (loop n [] lis);
};

let sort_result mode lis => {
  open List;
  switch mode {
  | `Last => sort (fun (x,y) (x',y') => x < x' ? 1 : -1) lis;
  | `First => sort (fun (x,y) (x',y') => x > x' ? 1 : -1) lis;
  | `None => lis;
  };
};

let take_from_memory n lis mode => {
  open List;
  let count = min n (length lis);
  let sorted = sort_result mode lis;
  (n - count, (take count sorted)) |> Lwt.return;
};


let read_memory ctx id n mode => {
  Lwt_io.printf "read_memory\n" >>= fun () =>
  Membuf.to_list ctx.membuf id >>= fun mem_shard =>
    take_from_memory n mem_shard mode;
};

let read_memory_all ctx id => {
  Membuf.exists ctx.membuf id ? Membuf.to_list ctx.membuf id : Lwt.return [];
};

let log_shard shard => {
  List.map (fun (_,json) => json) shard |> fun lis =>
    List.iter (fun elt => Printf.printf "%s\n" (Ezjsonm.to_string elt)) lis;
};


let read_disk ctx k n mode =>   {
  open List;
  let rec loop n acc lis => {
    switch lis {
    | [] => acc |> Lwt.return;
    | [tup, ...rest] =>
        Shard.get ctx.shard (make_key k tup) >>= fun shard => {
          let leftover = (n - length shard); 
          if (leftover > 0) {
            loop leftover (rev_append shard acc) rest;
          } else {
            rev_append (take n (sort_result mode shard)) acc |> Lwt.return;
          };
        };              
    };
  };
  Lwt_io.printf "read_disk\n" >>= fun () =>    
  Index.get ctx.index k >>= fun data =>
    switch (mode, data) {
    | (`Last, Some lis) => lis;
    | (`First, Some lis) => rev lis;  
    | (_, None) => [];
    } |> loop n [];
};

let to_json lis => {
  open Ezjsonm;
    List.map (fun (t,json) => dict [("timestamp", int t), ("data", value json)]) lis |> 
      fun lis' => `A lis';
};

let return_data sort::mode lis => {
  sort_result mode lis |> to_json |> Lwt.return;
};


let flush_memory ctx k => {
  read_memory_all ctx k >>=
    fun shard => flush_series ctx k shard;
};


let flush_memory_read_from_disk ctx k n mode => {
  Lwt_io.printf "flush_memory_read_from_disk\n" >>= 
    fun () => flush_memory ctx k >>=
      fun () => read_disk ctx k n mode;
};

let read_memory_then_disk ctx k n mode => {
  Lwt_io.printf "read_memory_then_disk\n" >>= fun () =>
  read_memory ctx k n mode >>= fun (leftover, mem) => {
    if (leftover > 0) {
      read_disk ctx k leftover mode >>= fun disk =>
        List.rev_append mem disk |> Lwt.return;
    } else {
      mem |> Lwt.return;
    };
  };
};

let read_last_worker ctx::ctx id::k n::n => {
  if (Membuf.exists ctx.membuf k) {
    switch (is_ascending ctx k) {
    | true => read_memory_then_disk ctx k n `Last;
    | false => flush_memory_read_from_disk ctx k n `Last;
    };
    } else {
      read_disk ctx k n `Last;
    };
};

let read_last ctx::ctx id::k n::n => {
  read_last_worker ctx::ctx id::k n::n >>=
    fun result => return_data sort::`Last result;
};

let read_lasts ctx::ctx id_list::id_list n::n => {
  Lwt_list.fold_left_s (fun acc id => 
    read_last_worker ctx::ctx id::id n::n >>=
      fun x => List.rev_append x acc |> Lwt.return) [] id_list >>=
        fun result => return_data sort::`Last result;   
};

let read_latest ctx::ctx id::k => {
  read_last ctx k 1;
};

let read_latests ctx::ctx id_list::id_list => {
  read_lasts ctx::ctx id_list::id_list n::1
};

let read_first_worker ctx::ctx id::k n::n => {
  if (Membuf.exists ctx.membuf k) {    
    switch (is_descending ctx k) { 
    | true => read_memory_then_disk ctx k n `First;
    | false => flush_memory_read_from_disk ctx k n `First;
    };
    } else {
      read_disk ctx k n `First;
  };
};

let read_first ctx::ctx id::k n::n => {
  read_first_worker ctx::ctx id::k n::n >>=
    fun result => return_data sort::`First result;
};

let read_firsts ctx::ctx id_list::id_list n::n => {
  Lwt_list.fold_left_s (fun acc id => 
    read_first_worker ctx::ctx id::id n::n >>=
      fun x => List.rev_append x acc |> Lwt.return) [] id_list >>=
        fun result => return_data sort::`First result;   
};

let read_earliest ctx::ctx id::k => {
  read_first ctx k 1;
};

let read_earliests ctx::ctx id_list::id_list => {
  read_firsts ctx::ctx id_list::id_list n::1
};

let number_of_records_on_disk ctx k lis => {
  Lwt_list.fold_left_s (fun acc x => 
    Shard.get ctx.shard (make_key k x) >>= 
      fun x => List.length x + acc |> Lwt.return) 0 lis;
};

let number_of_records_in_memory ctx k => {
  open Membuf;
  exists ctx.membuf k ? length ctx.membuf k : Lwt.return 0;
};


let length_of_json result => {
  open Ezjsonm;
  dict [("length", int result)] |> Lwt.return;
};

let length_worker ctx::ctx id::k => {
  open Ezjsonm;
  Index.get ctx.index k >>= fun data =>
    switch data {
    | Some lis => number_of_records_on_disk ctx k lis;
    | None => 0 |> Lwt.return;
    } >>= fun disk => 
      number_of_records_in_memory ctx k >>= 
        fun mem => Lwt.return (disk + mem);
};

let length ctx::ctx id::k => {
  length_worker ctx::ctx id::k >>= 
    fun result => length_of_json result;
};

let lengths ctx::ctx id_list::id_list => {
  open Ezjsonm;
  Lwt_list.fold_left_s (fun acc id => 
    length_worker ctx::ctx id::id >>=
      fun x => x + acc |> Lwt.return) 0 id_list >>=
        fun result => length_of_json result;
};

let make_filter_elt k tup mode => {
  (mode, make_key k tup);
};

let filter_since ts lis => {
  List.filter (fun (t,_) => t >= ts) lis;
};

let read_since_disk_worker ctx k ts status => {
  switch status {
  | `Complete => Shard.get ctx.shard k;
  | `Partial => Shard.get ctx.shard k >>= 
      fun shard => filter_since ts shard |> Lwt.return;
  };
};

let handle_read_since_disk ctx ts lis => {
  Lwt_list.fold_left_s (fun acc (status, key) => 
    read_since_disk_worker ctx key ts status >>= 
      fun x => List.rev_append x acc |> Lwt.return) [] lis;
};


let read_since_disk ctx k ts => {
  open List;
  let rec loop acc lis => {
    switch lis {
      | [] => acc;
      | [(lb,ub), ...rest] when lb >= ts && ub >= ts => 
          loop (cons (make_filter_elt k (lb,ub) `Complete) acc) rest; 
      | [(lb,ub), ...rest] when ub >= ts =>
          cons (make_filter_elt k (lb,ub) `Partial) acc;
      | [_, ...rest] => loop acc rest;
    };
  };
  Index.get ctx.index k >>= fun data =>
    switch data {
    | Some lis => lis;
    | None => [];
    } |> loop [] 
      |> handle_read_since_disk ctx ts;
        
};

let read_since_memory ctx k ts => {
  read_memory_all ctx k >>=
    fun data => filter_since ts data |> Lwt.return;
};

let read_since_worker ctx::ctx id::k from::ts => {
  read_since_memory ctx k ts >>= fun mem =>
    read_since_disk ctx k ts >>= fun disk => 
      List.rev_append mem disk |> Lwt.return;
};

let read_since ctx::ctx id::k from::ts => {
  read_since_worker ctx::ctx id::k from::ts >>=
    fun result => return_data sort::`Last result;
};


let read_sinces ctx::ctx id_list::id_list from::ts => {
  Lwt_list.fold_left_s (fun acc id => 
    read_since_worker ctx::ctx id::id from::ts >>=
      fun x => List.rev_append x acc |> Lwt.return) [] id_list >>=
        fun result => return_data sort::`Last result;   
};


let filter_until ts lis => {
  List.filter (fun (t,_) => t <= ts) lis;
};

let read_range_worker ctx::ctx id::k from::t1 to::t2 => {
  read_since_memory ctx k t1 >>= fun mem =>
    read_since_disk ctx k t1 >>= fun disk =>
      List.rev_append mem disk |> filter_until t2 |> Lwt.return;
};

let read_range ctx::ctx id::k from::t1 to::t2 => {
  read_range_worker ctx::ctx id::k from::t1 to::t2 >>=
    fun result => return_data sort::`Last result;
};

let read_ranges ctx::ctx id_list::id_list from::t1 to::t2 => {
  Lwt_list.fold_left_s (fun acc id => 
    read_range_worker ctx::ctx id::id from::t1 to::t2 >>=
      fun x => List.rev_append x acc |> Lwt.return) [] id_list >>=
        fun result => return_data sort::`Last result;   
};


let get_timestamps json => {
  open Ezjsonm;
  List.rev_map (fun x => get_int x) (get_list (fun x => find x ["timestamp"]) json);
};


let filter_shard_worker ctx key timestamps => {
  Shard.get ctx.shard key >>= 
    fun lis => List.filter (fun (t,_) => not (List.mem t timestamps)) lis |>
      fun lis' => Shard.add ctx.shard key lis';
};

let delete_worker ctx key_list timestamps => {
  Lwt_list.iter_s (fun k => filter_shard_worker ctx k timestamps) key_list;
};

let delete ctx::ctx id_list::id_list json::json => {
  Lwt_list.iter_s (fun id => flush_memory ctx id) id_list >>= 
    fun () => json >>= fun json' => {
      let timestamps = get_timestamps (Ezjsonm.value json');
      let keys = Lwt_list.map_s (fun k => list_of_keys ctx k) id_list;
        keys >>= fun keys' => {
        Lwt_list.iter_s (fun k => delete_worker ctx k timestamps) keys';
      };
    };
};