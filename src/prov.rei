type t;
let create : code::int => options::array (int,string) => token::string => t;
let ident : t => (string, int);