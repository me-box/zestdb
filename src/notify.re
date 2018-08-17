
type t = {mutable notify_list: list(string)};

let create = () => {notify_list: []};

let add = (ctx, ident) => {
  ctx.notify_list = List.cons(ident, ctx.notify_list);
};

let get_all = (ctx) => {
  ctx.notify_list;
};