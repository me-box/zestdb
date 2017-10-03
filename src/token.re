module Macaroon = Sodium_macaroons;

let mint_token ::id ::location ::path ::method ::target ::key => {
    let m = Macaroon.create ::id ::location ::key;
    let m = Macaroon.add_first_party_caveat m path;
    let m = Macaroon.add_first_party_caveat m method;
    let m = Macaroon.add_first_party_caveat m target;
    Macaroon.serialize m
};

let is_valid token key caveats => {
  let check_caveats s => List.mem s caveats;  
  let validate m secret => Macaroon.verify m key::secret check::check_caveats [];
  switch (Macaroon.deserialize token) {
  | `Ok m => validate m key
  | `Error _ => false;
  };
};
