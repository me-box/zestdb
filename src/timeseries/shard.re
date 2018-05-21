open Lwt.Infix;

module Store = Ezirmin.FS_lww_register (Tc.List(Tc.Pair(Tc.Int)(Irmin.Contents.Json)));

type t = Store.branch;

let create ::file => {
  Store.init root::file bare::true () >>= Store.master;
};


let write branch info k v => {
  branch >>= fun branch' =>
    Store.write message::info branch' path::k v;
};


let read branch k => {
  branch >>= fun branch' =>
    Store.read branch' path::k;
};


let add branch info k v => {
  write branch info k v;
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

let remove branch info key_list => {
  Lwt_list.iter_s (fun k => write branch info k []) key_list;
};
