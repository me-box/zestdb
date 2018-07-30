open Lwt.Infix;

module Store = Ezirmin.FS_lww_register((Tc.List((Tc.Pair(Tc.Int, Tc.Int)))));

type t = Store.branch;

let create = (~file) => Store.init(~root=file, ~bare=true, ()) >>= Store.master;

let write = (branch, info, k, v) =>
  branch >>= ((branch') => Store.write(~message=info, branch', ~path=[k], v));

let read = (branch, k) => branch >>= ((branch') => Store.read(branch', ~path=[k]));

let tup_sort = (lis) => {
  let cmp = ((x, y), (x', y')) => y < y' ? 1 : (-1);
  List.sort(cmp, lis);
};

let add_tuple = (tup, lis) => List.cons(tup, lis) |> tup_sort;

let tuple_exists = (tup, lis) => List.exists((t) => t == tup, lis);

let since = (t1, lis) => List.filter(((t1', t2')) => t1 <= t1', lis);

let until = (t2, lis) => List.filter(((t1', t2')) => t2 >= t1', lis);

let range = ((t1, t2), lis) => since(t1, lis) |> until(t2);

let bounds = (lis) =>
  if (lis == []) {
    None;
  } else {
    let xs = List.map(((x, _)) => x, lis) |> Array.of_list;
    let ys = List.map(((_, y)) => y, lis) |> Array.of_list;
    let x = Oml.Util.Array.min(xs);
    let y = Oml.Util.Array.max(ys);
    Some((x, y));
  };

let filter_list = (rem_lis, lis) => {
  open List;
  let rec loop = (acc, l) =>
    switch l {
    | [] => acc
    | [x, ...xs] => mem(x, rem_lis) ? loop(acc, xs) : loop(cons(x, acc), xs)
    };
  loop([], lis);
};

let update = (branch, info, k, tup, remove_list) =>
  List.(
    read(branch, k)
    >>= (
      (data) =>
        switch data {
        | Some((curr_lis)) =>
          let filtered = filter_list(remove_list, curr_lis);
          let new_index = add_tuple(tup, filtered);
          write(branch, info, k, new_index) >>= (() => bounds(new_index) |> Lwt.return);
        | None => write(branch, info, k, [tup]) >>= (() => bounds([tup]) |> Lwt.return)
        }
    )
  );

let get = (branch, k) =>
  read(branch, k)
  >>= (
    (data) =>
      (
        switch data {
        | Some((lis)) => Some(lis)
        | None => None
        }
      )
      |> Lwt.return
  );

let overlap_worker = (index, lis) => {
  let (x, y) = index;
  List.filter(((x', y')) => x <= y' && y >= x', lis);
};

let overlap = (branch, k, index) =>
  List.(
    read(branch, k)
    >>= (
      (data) =>
        (
          switch data {
          | Some((lis)) => overlap_worker(index, lis)
          | None => []
          }
        )
        |> Lwt.return
    )
  );

let range = (branch, k) =>
  List.(
    read(branch, k)
    >>= (
      (data) =>
        (
          switch data {
          | None => None
          | Some((lis)) => bounds(lis)
          }
        )
        |> Lwt.return
    )
  );