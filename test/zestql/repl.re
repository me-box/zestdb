open Printf;

open Lexer;

open Lexing;

let print_position = (outx, lexbuf) => {
  let pos = lexbuf.lex_curr_p;
  fprintf(outx, "%s:%d:%d", pos.pos_fname, pos.pos_lnum, pos.pos_cnum - pos.pos_bol + 1);
};

let parse_with_error = (lexbuf) =>
  try (Parser.lang(Lexer.read, lexbuf)) {
  | SyntaxError(msg) =>
    fprintf(stderr, "%a: %s\n", print_position, lexbuf, msg);
    None;
  | Parser.Error =>
    fprintf(stderr, "%a: syntax error\n", print_position, lexbuf);
    exit(-1);
  };

[@part "1"];

let rec parse_and_print = (lexbuf) => {
  switch (parse_with_error(lexbuf)) {
  | Some(value) => {
      let res = Zestql.process(value);
      printf("> %s\n", res);
      parse_and_print(lexbuf);
    };
  | None => printf("done\n")
  };
};

let parse_file = (filename) => {
  let inx = open_in(filename);
  let lexbuf = Lexing.from_channel(inx);
  lexbuf.lex_curr_p = {...lexbuf.lex_curr_p, pos_fname: filename};
  parse_and_print(lexbuf);
  close_in(inx);
};

let parse_string = (s) => {
  let lexbuf = Lexing.from_string(s);
  parse_and_print(lexbuf);
};