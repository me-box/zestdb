open Lwt.Infix;


type t = {
  ht: Membufhash.t,
  m: Lwt_mutex.t
};

let create () => {
  {
    ht: Membufhash.create (),
    m: Lwt_mutex.create ()
  };
};

let serialise_worker ctx key => {
  let q = Membufhash.get ctx.ht key;
  (key, Membufq.to_list q);
};

let serialise ctx => {
  let keys = Membufhash.get_keys ctx.ht;
  List.map (fun k => serialise_worker ctx k) keys |> Lwt.return;
};

let empty_series ctx key => {
  let q = Membufhash.get ctx.ht key;
  Membufq.clear q;
  Lwt.return_unit;
};

let empty ctx => {
  let keys = Membufhash.get_keys ctx.ht;
  Lwt_list.iter_s (fun k => empty_series ctx k) keys;
};


let handle_write ctx id elt => {
  if (Membufhash.exists ctx.ht id) {
    let q = Membufhash.get ctx.ht id;
    Membufq.push q elt;
    Membufhash.replace ctx.ht id q;
  } else {
    let q = Membufq.create ();
    Membufq.push q elt;
    Membufhash.add ctx.ht id q;
  };
};

let write ctx id elt => {
  Lwt_mutex.lock ctx.m >>= fun () =>
    handle_write ctx id elt |> fun () => 
      Lwt_mutex.unlock ctx.m |> Lwt.return;
};

let read ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.pop q |> Lwt.return;
};

let length ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.length q |> Lwt.return;  
};

let to_list ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.to_list q |> Lwt.return;  
};

let set_disk_range ctx id range => {
  let q = Membufhash.get ctx.ht id;
  Membufq.set_disk_range q range |> Lwt.return;
};

let get_disk_range ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.get_disk_range q |> Lwt.return;
};

let set_ascending_series ctx id v => {
  let q = Membufhash.get ctx.ht id;
  Membufq.set_ascending_series q v |> Lwt.return;
};

let get_ascending_series ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.get_ascending_series q |> Lwt.return;
};

let set_descending_series ctx id v => {
  let q = Membufhash.get ctx.ht id;
  Membufq.set_descending_series q v |> Lwt.return;
};

let get_descending_series ctx id => {
  let q = Membufhash.get ctx.ht id;
  Membufq.get_descending_series q |> Lwt.return;
};