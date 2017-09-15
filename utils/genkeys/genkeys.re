let (public_key,private_key) = ZMQ.Curve.keypair ();
Printf.printf "public key: '%s'\n" public_key;
Printf.printf "private key: '%s'\n" private_key;