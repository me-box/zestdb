open Lwt.Infix;

module Store = Ezirmin.FS_lww_register (Tc.List(Tc.Pair(Tc.Int)(Irmin.Contents.Json)));

type t = Store.branch;

let create ::file => {
  Store.init root::file bare::true () >>= Store.master;
};


let write branch k v => {
  branch >>= fun branch' =>
    Store.write message::"write shard" branch' path::k v;
};


let read branch k => {
  branch >>= fun branch' =>
    Store.read branch' path::k;
};


let add branch k v => {
  write branch k v;
};

let sort_shard lis => {
  let cmp (x,y) (x',y') => x < x' ? 1 : -1;
  List.sort cmp lis;
};

let get branch k => {
  read branch k >>= fun shard =>
    switch shard {
    | Some lis => lis |> sort_shard;
    | None => [];
    } |> Lwt.return;
};

let remove branch key_list => {
  Lwt_list.iter_s (fun k => write branch k []) key_list;
};
