open Lwt.Infix;

type t = {
  code: int,
  uri_path: string,
  uri_host: string,
  content_format: int,
  token: string
};


let create code::code options::options token::token => {
  open Protocol.Zest;
  {
    code: code,
    uri_path: get_uri_path options,
    uri_host: get_uri_host options,
    content_format: get_content_format options,
    token: token
  };
};

let ident t => {
  (t.uri_path, t.content_format);
};