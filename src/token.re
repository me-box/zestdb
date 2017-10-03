module Macaroon = Sodium_macaroons;

let is_valid token key caveats => {
  let check_caveats s => List.mem s caveats;  
  let validate m secret => Macaroon.verify m key::secret check::check_caveats [];
  switch (Macaroon.deserialize token) {
  | `Ok m => validate m key
  | `Error _ => false;
  };
};
