%token <int> INT
%token <string> STRING
%token <float> FLOAT
%token CONNECT
%token DISCONNECT
%token POST
%token TO
%token WHERE
%token IS
%token HOST
%token GET
%token DELETE
%token FROM
%token SINCE
%token RANGE
%token EOF
%token SEMI_COLON
%token MIN
%token MAX
%token SUM
%token COUNT
%token MEAN
%token SD
%token OBSERVE
%token FOR
%token LAST
%token MODE
%token DATA
%token AUDIT
%token KEY
%token SECONDS
%token MINUTES
%token HOURS
%token DAYS

%start <Zestql.value option> lang
%%

lang:
  | v = statement; SEMI_COLON { Some v }
  | EOF       { None   } ;

statement:
  | CONNECT; key = key; host = host? { `Connect (host, key) }
  | POST; num = FLOAT; to_ = to_; tag = tag?;  { `Post (num, to_, tag) }
  | GET; func = func?; from = from; tag = tag?; since = since; { `Get_since (func,from,tag,since) }
  | GET; func = func?; from = from; tag = tag?; range = range; { `Get_range (func,from,tag,range) }
  | GET; func = func?; from = from; tag = tag?; last = last; { `Get_last (func,from,tag,last) }
  | OBSERVE; from = from; mode = observe_mode?; max_age = max_age?; { `Observe (from,mode,max_age) }
  | DELETE; from = from; tag = tag?; range = range; { `Delete_range (from,tag,range) }
  | DISCONNECT; host = host? { `Disconnect (host) }


host:
  HOST; s = STRING { s };

key:
  KEY; s = STRING { s };  

func:
  MIN { "min" } | MAX; { "max"} | SUM; {"sum"} | COUNT; {"count"} | MEAN; {"mean"} | SD; {"sd"};

from:
  FROM; s = STRING { s };
  
to_:
  TO; s = STRING { s };

range:
  | RANGE; n1 = INT; SECONDS; TO; n2 = INT; SECONDS { (Zestql.get_seconds(n1), Zestql.get_seconds(n2))}
  | RANGE; n1 = INT; MINUTES; TO; n2 = INT; MINUTES { (Zestql.get_minutes(n1), Zestql.get_minutes(n2))}
  | RANGE; n1 = INT; HOURS; TO; n2 = INT; HOURS { (Zestql.get_hours(n1), Zestql.get_hours(n2))}
  | RANGE; n1 = INT; DAYS; TO; n2 = INT; DAYS { (Zestql.get_days(n1), Zestql.get_days(n2))} ;

last:
  LAST; n = INT { n };

tag:
  WHERE; s1 = STRING; IS; s2 = STRING { (s1,s2) } ;

max_age:
  FOR; n = INT; SECONDS { n } ;

observe_mode:
  MODE; DATA; { "data" } | MODE; AUDIT { "audit" } ;

since:
  | SINCE; n = INT; SECONDS { Zestql.get_seconds(n) }
  | SINCE; n = INT; MINUTES { Zestql.get_minutes(n) }
  | SINCE; n = INT; HOURS { Zestql.get_hours(n) }
  | SINCE; n = INT; DAYS { Zestql.get_days(n) } ;
