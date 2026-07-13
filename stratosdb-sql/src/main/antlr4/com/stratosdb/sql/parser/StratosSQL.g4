grammar StratosSQL;

// Parser rules
parse: sqlStatement EOF;

sqlStatement: createTable | insert | select | update | delete | dropTable | showTables;

// DDL
createTable: CREATE TABLE tableName LPAREN columnDef (COMMA columnDef)* RPAREN SEMICOLON?;
dropTable: DROP TABLE tableName SEMICOLON?;
showTables: SHOW TABLES SEMICOLON?;

// DML
insert: INSERT INTO tableName (LPAREN columnName (COMMA columnName)* RPAREN)? VALUES LPAREN valueList RPAREN SEMICOLON?;
select: SELECT selectList FROM tableName (WHERE expression)? (ORDER BY orderList)? (LIMIT limitValue)? SEMICOLON?;
update: UPDATE tableName SET assignment (COMMA assignment)* (WHERE expression)? SEMICOLON?;
delete: DELETE FROM tableName (WHERE expression)? SEMICOLON?;

// Definitions
columnDef: columnName dataType (NOT NULL)? (DEFAULT defaultValue)?;
assignment: columnName ASSIGN literal;
orderList: orderItem (COMMA orderItem)*;
orderItem: columnName (ASC | DESC)?;

// Select list
selectList: STAR | selectItem (COMMA selectItem)*;
selectItem: expression (AS alias)? | columnName (AS alias)?;

// Expressions
expression: columnName ASSIGN literal
           | columnName GT literal
           | columnName LT literal
           | columnName GE literal
           | columnName LE literal
           | columnName NE literal
           | columnName LIKE literal
           | columnName IN LPAREN valueList RPAREN
           | LPAREN expression RPAREN AND expression
           | LPAREN expression RPAREN OR expression
           | NOT expression;

// Values
valueList: literal (COMMA literal)*;

// Data types
dataType: INT | INTEGER | BIGINT | SMALLINT | TINYINT
        | VARCHAR LPAREN INTEGER_LITERAL RPAREN
        | TEXT | CHAR LPAREN INTEGER_LITERAL RPAREN
        | BOOLEAN | BOOL
        | DATE | TIME | TIMESTAMP
        | DECIMAL LPAREN INTEGER_LITERAL COMMA INTEGER_LITERAL RPAREN
        | DOUBLE | FLOAT
        | BYTEA | BLOB
        | UUID
        | JSON | JSONB;

// Literals
literal: STRING_LITERAL | INTEGER_LITERAL | FLOAT_LITERAL | BOOLEAN_LITERAL | NULL_LITERAL;
defaultValue: literal | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP;

// Identifiers
tableName: IDENTIFIER;
columnName: IDENTIFIER;
alias: IDENTIFIER | STRING_LITERAL;
limitValue: INTEGER_LITERAL;

// Lexer rules
CREATE: C R E A T E;
TABLE: T A B L E;
DROP: D R O P;
INSERT: I N S E R T;
INTO: I N T O;
VALUES: V A L U E S;
SELECT: S E L E C T;
FROM: F R O M;
WHERE: W H E R E;
UPDATE: U P D A T E;
SET: S E T;
DELETE: D E L E T E;
ORDER: O R D E R;
BY: B Y;
LIMIT: L I M I T;
ASC: A S C;
DESC: D E S C;
AND: A N D;
OR: O R;
NOT: N O T;
IN: I N;
LIKE: L I K E;
IS: I S;
NULL: N U L L;
DEFAULT: D E F A U L T;
SHOW: S H O W;
TABLES: T A B L E S;

// Data type keywords
INT: I N T;
INTEGER: I N T E G E R;
BIGINT: B I G I N T;
SMALLINT: S M A L L I N T;
TINYINT: T I N Y I N T;
VARCHAR: V A R C H A R;
TEXT: T E X T;
CHAR: C H A R;
BOOLEAN: B O O L E A N;
BOOL: B O O L;
DATE: D A T E;
TIME: T I M E;
TIMESTAMP: T I M E S T A M P;
DECIMAL: D E C I M A L;
DOUBLE: D O U B L E;
FLOAT: F L O A T;
BYTEA: B Y T E A;
BLOB: B L O B;
UUID: U U I D;
JSON: J S O N;
JSONB: J S O N B;

CURRENT_DATE: C U R R E N T '_' D A T E;
CURRENT_TIME: C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;

// Operators and symbols
LPAREN: '(';
RPAREN: ')';
COMMA: ',';
SEMICOLON: ';';
ASSIGN: '=';
GT: '>';
LT: '<';
GE: '>=';
LE: '<=';
NE: '!=';
STAR: '*';

// Literals
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;
STRING_LITERAL: '\'' (~['])* '\'';
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;
BOOLEAN_LITERAL: TRUE | FALSE;
NULL_LITERAL: N U L L;

// Boolean literals (must be defined as lexer tokens)
TRUE: T R U E;
FALSE: F A L S E;

// Fragment rules for case-insensitivity
fragment A: [aA];
fragment B: [bB];
fragment C: [cC];
fragment D: [dD];
fragment E: [eE];
fragment F: [fF];
fragment G: [gG];
fragment H: [hH];
fragment I: [iI];
fragment J: [jJ];
fragment K: [kK];
fragment L: [lL];
fragment M: [mM];
fragment N: [nN];
fragment O: [oO];
fragment P: [pP];
fragment Q: [qQ];
fragment R: [rR];
fragment S: [sS];
fragment T: [tT];
fragment U: [uU];
fragment V: [vV];
fragment W: [wW];
fragment X: [xX];
fragment Y: [yY];
fragment Z: [zZ];

// Skip whitespace
WS: [ \t\r\n]+ -> skip;