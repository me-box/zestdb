open Lwt.Infix;
open Lwt_io;
open Lexer;
open Lexing;


let error_info = (lexbuf) => {
  let pos = lexbuf.lex_curr_p;
  Printf.sprintf("%s:%d:%d", pos.pos_fname, pos.pos_lnum, pos.pos_cnum - pos.pos_bol + 1);
};

let parse_with_error = (lexbuf) => {
  try (Parser.lang(Lexer.read, lexbuf)) {
  | SyntaxError(msg) => {
      let info = error_info(lexbuf);
      let _ = fprintf(stderr, "%s %s", info, msg);
      None;
    };
  | Parser.Error => {
      let info = error_info(lexbuf);
      let _ = fprintf(stderr, "%s syntax error", info);
      None;
    };
  };
};

let parse = (lexbuf) => {
  switch (parse_with_error(lexbuf)) {
  | Some(value) => Zestql.process(value);
  | None => "";
  };
};

let parse_string = (s) => {
  let lexbuf = Lexing.from_string(s); 
  parse(lexbuf) |> Lwt.return;
};

let rec repl = () => {
  printf("> ") >>= () =>
    read_line(stdin) >>=
      line => switch(line) {
      | "quit" | "quit;" => exit(0);
      | "" => repl();
      | _ => parse_string(line) >>= 
              res => write_line(stdout, res) >>= 
                () => repl();
      };
};

let rec run_repl = () => {
  try (Lwt_main.run(repl())) {
  | _ => let _ = fprintf(stderr, "Unkown error\n");
  };
  run_repl();
};

printf("zestql v0.1\n") >>= () => run_repl();