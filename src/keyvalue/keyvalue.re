open Lwt.Infix;

module Json = {
  module Store = Ezirmin.FS_lww_register(Irmin.Contents.Json);
  type t = {
    branch: Lwt.t(Store.branch),
    keys: Keys.t
  };
  let json_empty = Ezjsonm.dict([]);
  let create = (~path_to_db as path) => {
    branch: Store.init(~root=path ++ "Json_kv_store", ~bare=true, ()) >>= Store.master,
    keys: Keys.create(~file=path ++ "Json_kv_keys_store")
  };
  let write = (~ctx, ~info, ~id, ~key as k, ~json as v) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.write(~message=info, branch', ~path=[id, k], v)
        >>= (() => Keys.update(ctx.keys, info, id, k))
    );
  let read = (~ctx, ~info, ~id, ~key as k) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.read(branch', ~path=[id, k])
        >>= (
          (data) =>
            switch data {
            | Some((json)) => Lwt.return(json)
            | None => Lwt.return(json_empty)
            }
        )
    );
  let keys = (~ctx, ~info, ~id) => Keys.get(ctx.keys, id);
  let count_of_json = (result) => Ezjsonm.(dict([("count", int(result))]) |> Lwt.return);
  let count = (~ctx, ~info, ~id) => Keys.count(ctx.keys, id) >>= count_of_json;
  let delete = (~ctx, ~info, ~id, ~key as k) =>
    write(~ctx=ctx, ~info=info, ~id=id, ~key=k, ~json=json_empty)
    >>= (() => Keys.delete(ctx.keys, info, id, k));
  let delete_all = (~ctx, ~info, ~id) =>
    Keys.alist(ctx.keys, id)
    >>= ((lis) => Lwt_list.iter_s((k) => delete(~ctx=ctx, ~info=info, ~id=id, ~key=k), lis));
};

module Text = {
  module Store = Ezirmin.FS_lww_register(Irmin.Contents.String);
  type t = {
    branch: Lwt.t(Store.branch),
    keys: Keys.t
  };
  let text_empty = "";
  let create = (~path_to_db as path) => {
    branch: Store.init(~root=path ++ "Text_kv_store", ~bare=true, ()) >>= Store.master,
    keys: Keys.create(~file=path ++ "Text_kv_keys_store")
  };
  let write = (~ctx, ~info, ~id, ~key as k, ~text as v) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.write(~message=info, branch', ~path=[id, k], v)
        >>= (() => Keys.update(ctx.keys, info, id, k))
    );
  let read = (~ctx, ~info, ~id, ~key as k) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.read(branch', ~path=[id, k])
        >>= (
          (data) =>
            switch data {
            | Some((json)) => Lwt.return(json)
            | None => Lwt.return(text_empty)
            }
        )
    );
  let keys = (~ctx, ~info, ~id) => Keys.get(ctx.keys, id);
  let count_of_json = (result) => Ezjsonm.(dict([("count", int(result))]) |> Lwt.return);
  let count = (~ctx, ~info, ~id) => Keys.count(ctx.keys, id) >>= count_of_json;
  let delete = (~ctx, ~info, ~id, ~key as k) =>
    write(~ctx=ctx, ~info=info, ~id=id, ~key=k, ~text=text_empty)
    >>= (() => Keys.delete(ctx.keys, info, id, k));
  let delete_all = (~ctx, ~info, ~id) =>
    Keys.alist(ctx.keys, id)
    >>= ((lis) => Lwt_list.iter_s((k) => delete(~ctx=ctx, ~info=info, ~id=id, ~key=k), lis));
};

module Binary = {
  module Store = Ezirmin.FS_lww_register(Irmin.Contents.String);
  type t = {
    branch: Lwt.t(Store.branch),
    keys: Keys.t
  };
  let binary_empty = "";
  let create = (~path_to_db as path) => {
    branch: Store.init(~root=path ++ "Binary_kv_store", ~bare=true, ()) >>= Store.master,
    keys: Keys.create(~file=path ++ "Binary_kv_key_store")
  };
  let write = (~ctx, ~info, ~id, ~key as k, ~binary as v) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.write(~message=info, branch', ~path=[id, k], v)
        >>= (() => Keys.update(ctx.keys, info, id, k))
    );
  let read = (~ctx, ~info, ~id, ~key as k) =>
    ctx.branch
    >>= (
      (branch') =>
        Store.read(branch', ~path=[id, k])
        >>= (
          (data) =>
            switch data {
            | Some((json)) => Lwt.return(json)
            | None => Lwt.return(binary_empty)
            }
        )
    );
  let keys = (~ctx, ~info, ~id) => Keys.get(ctx.keys, id);
  let count_of_json = (result) => Ezjsonm.(dict([("count", int(result))]) |> Lwt.return);
  let count = (~ctx, ~info, ~id) => Keys.count(ctx.keys, id) >>= count_of_json;
  let delete = (~ctx, ~info, ~id, ~key as k) =>
    write(~ctx=ctx, ~info=info, ~id=id, ~key=k, ~binary=binary_empty)
    >>= (() => Keys.delete(ctx.keys, info, id, k));
  let delete_all = (~ctx, ~info, ~id) =>
    Keys.alist(ctx.keys, id)
    >>= ((lis) => Lwt_list.iter_s((k) => delete(~ctx=ctx, ~info=info, ~id=id, ~key=k), lis));
};