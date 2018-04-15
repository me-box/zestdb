open Lwt.Infix;


type t = {
  q: Queue.t (int, Ezjsonm.t),
  mutable disk_range: option (int, int),
  mutable ascending_series: bool,
  mutable descending_series: bool
};

let create () => {
  {
    q: Queue.create (),
    disk_range: None,
    ascending_series: true,
    descending_series: true
  };
};

let push ctx n => {
  Queue.push n ctx.q;
};

let pop ctx => {
  Queue.pop ctx.q;
};

let length ctx => {
  Queue.length ctx.q;
};

let to_list ctx => {
  Queue.fold (fun x y => List.cons y x) [] ctx.q;
};


let is_ascending_queue ctx => {
  let rec is_sorted lis => {
    switch lis {
    | [x, y, ...l] => x <= y && is_sorted [y, ...l]
    | _ => true;
    };
  };
  is_sorted (to_list ctx);
};

let is_descending_queue ctx => {
  let rec is_sorted lis => {
    switch lis {
    | [x, y, ...l] => x >= y && is_sorted [y, ...l]
    | _ => true;
    };
  };
  is_sorted (to_list ctx);
};

let clear ctx => {
  Queue.clear ctx.q;
};

let set_disk_range ctx range => {
  ctx.disk_range = range;
};

let get_disk_range ctx => {
  ctx.disk_range;
};

let set_ascending_series ctx v => {
  ctx.ascending_series = v;
};

let get_ascending_series ctx => {
  ctx.ascending_series;
};

let set_descending_series ctx v => {
  ctx.descending_series = v;
};

let get_descending_series ctx => {
  ctx.descending_series;
};