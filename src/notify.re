
type t = {mutable notify_list: list(string)};

let create = () => {notify_list: []};

let exists = (ctx, ident) => {
  List.exists(x => x == ident, ctx.notify_list);
};

let add = (ctx, ident) => {
  if (exists(ctx, ident)) {
    false;
  } else {
    ctx.notify_list = List.cons(ident, ctx.notify_list);
    true;
  };
};

let get_all = (ctx) => {
  ctx.notify_list;
};