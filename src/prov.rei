type t;
let create : code::int => options::array (int,string) => token::string => t;
let ident : t => (string, int);
let code : t => int;
let uri_path : t => string;
let uri_host : t => string;
let content_format : t => int;
let token : t => string;