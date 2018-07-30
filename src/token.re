module Macaroon = Sodium_macaroons;

let has_prefix = (s1, s2) =>
  String.(length(s1) <= length(s2) && s1 == sub(s2, 0, length(s1) - 1) ++ "*");

let check = (s, caveats) =>
  List.(
    if (Str.last_chars(s, 1) == "*") {
      exists((s') => has_prefix(s, s'), caveats);
    } else {
      mem(s, caveats);
    }
  );

let is_valid = (token, key, caveats) => {
  let check_caveats = (s) => check(s, caveats);
  let validate = (m, secret) => Macaroon.verify(m, ~key=secret, ~check=check_caveats, []);
  switch (Macaroon.deserialize(token)) {
  | `Ok(m) => validate(m, key)
  | `Error(_) => false
  };
};