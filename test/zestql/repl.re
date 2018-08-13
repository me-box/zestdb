open Lwt.Infix;
open Printf;
open Lexer;
open Lexing;


let parse = (lexbuf) => {
  switch (Parser.lang(Lexer.read, lexbuf)) {
  | Some(value) => Zestql.process(value);
  | None => "";
  };
};

let parse_string = (s) => {
  let lexbuf = Lexing.from_string(s); 
  parse(lexbuf) |> Lwt.return;
};



let rec repl = () => {
  Lwt_io.printf("> ") >>= () =>
    Lwt_io.(read_line(stdin)) >>=
      line => switch(line) {
      | "quit" | "quit;" => exit(0);
      | _ => parse_string(line) >>= 
              res => Lwt_io.(write_line(stdout, res)) >>= 
                () => repl();
      };
};

let rec run_repl = () => {
  try (Lwt_main.run(repl())) {
  | SyntaxError(msg) => let _ = Lwt_io.printf("Syntax error\n");
  | Parser.Error => let _ = Lwt_io.printf("Syntax error\n");
  | _ => let _ = Lwt_io.printf("Ouch\n");
  };
  run_repl();
};

run_repl();