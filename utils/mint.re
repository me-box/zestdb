module Macaroon = Sodium_macaroons;

let mint_token ::id ::location ::path ::meth ::target ::key => {
    let m = Macaroon.create ::id ::location ::key;
    let m = Macaroon.add_first_party_caveat m path;
    let m = Macaroon.add_first_party_caveat m meth;
    let m = Macaroon.add_first_party_caveat m target;
    Macaroon.serialize m
};

let id = ref "";
let location = ref "";
let path = ref "";
let meth = ref "";
let target = ref "";
let key = ref "";

let parse_cmdline () => { 
    let usage = "usage: " ^ Sys.argv.(0);
    let speclist = [
      ("--id", Arg.Set_string id, ": to give the token an id"),
      ("--location", Arg.Set_string location, ": to set a location"),
      ("--path", Arg.Set_string path, ": to set the allowed path"),
      ("--method", Arg.Set_string meth, ": to set the allowed method"),
      ("--target", Arg.Set_string target, ": to set target host identity of the token"),
      ("--key", Arg.Set_string key, ": to set encrypt the token"),
    ];
    Arg.parse speclist (fun err => raise (Arg.Bad ("Bad argument : " ^ err))) usage;
  };

  parse_cmdline ();
  let token = mint_token id::!id location::!location path::!path meth::!meth target::!target key::!key;
  Printf.printf "%s\n" token;