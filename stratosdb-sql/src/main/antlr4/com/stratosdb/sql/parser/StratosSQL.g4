grammar StratosSQL;

// Parser rules
parse: sqlStatement EOF;

sqlStatement: createTable | createIndex | insert | selectWithCte | select | update | delete | dropTable | showTables | showStats | explain | analyze | vacuum | beginTxn | commitTxn | rollbackTxn | createView | dropView | savepoint | releaseSavepoint | rollbackToSavepoint | createSequence | dropSequence;

// DDL
createTable: CREATE TABLE tableName LPAREN columnDef (COMMA columnDef)* RPAREN SEMICOLON?;
createIndex: CREATE INDEX indexName ON tableName LPAREN columnName (COMMA columnName)? RPAREN (USING (HASH | BTREE | BRIN | GIN | BITMAP | GIST))? SEMICOLON?;
dropTable: DROP TABLE tableName SEMICOLON?;
createView: CREATE VIEW viewName AS select SEMICOLON?;
dropView: DROP VIEW viewName SEMICOLON?;
viewName: IDENTIFIER;
showTables: SHOW TABLES SEMICOLON?;
showStats: SHOW STATS SEMICOLON?;
createSequence: CREATE SEQUENCE sequenceName (START WITH? INTEGER_LITERAL)? (INCREMENT BY? INTEGER_LITERAL)? SEMICOLON?;
dropSequence: DROP SEQUENCE sequenceName SEMICOLON?;
sequenceName: IDENTIFIER;
explain: EXPLAIN select;
analyze: ANALYZE tableName SEMICOLON?;
vacuum: VACUUM tableName SEMICOLON?;
beginTxn: BEGIN TRANSACTION? SEMICOLON? | START TRANSACTION SEMICOLON?;
commitTxn: COMMIT SEMICOLON?;
rollbackTxn: ROLLBACK SEMICOLON?;
savepoint: SAVEPOINT savepointName SEMICOLON?;
releaseSavepoint: RELEASE SAVEPOINT? savepointName SEMICOLON?;
rollbackToSavepoint: ROLLBACK TO SAVEPOINT? savepointName SEMICOLON?;
savepointName: IDENTIFIER;

// DML
insert: INSERT INTO tableName (LPAREN columnName (COMMA columnName)* RPAREN)? VALUES LPAREN valueList RPAREN SEMICOLON?;
select: SELECT selectList FROM tableName joinClause* (WHERE expression)? (GROUP BY groupByList)? (HAVING havingClause)? (ORDER BY orderList)? (LIMIT limitValue)? SEMICOLON?;
selectWithCte: WITH RECURSIVE? cteName AS LPAREN select (UNION ALL select)? RPAREN select;
cteName: IDENTIFIER;
joinClause: (INNER)? JOIN tableName ON columnName ASSIGN columnName;
groupByList: columnName (COMMA columnName)*;
havingClause: aggregateFunction ASSIGN literal
            | aggregateFunction GT literal
            | aggregateFunction LT literal
            | aggregateFunction GE literal
            | aggregateFunction LE literal
            | aggregateFunction NE literal;
aggregateFunction: (COUNT | SUM | AVG | MIN | MAX) LPAREN (STAR | columnName) RPAREN;
update: UPDATE tableName SET assignment (COMMA assignment)* (WHERE expression)? SEMICOLON?;
delete: DELETE FROM tableName (WHERE expression)? SEMICOLON?;

// Definitions
columnDef: columnName dataType (NOT NULL)? (DEFAULT defaultValue)?;
assignment: columnName ASSIGN literal;
orderList: orderItem (COMMA orderItem)*;
orderItem: columnName (ASC | DESC)?;
indexName: IDENTIFIER;

// Select list
selectList: STAR | selectItem (COMMA selectItem)*;
selectItem: windowFunction (AS alias)? | aggregateFunction (AS alias)? | expression (AS alias)? | columnName (AS alias)?;
windowFunction: (ROW_NUMBER | RANK | DENSE_RANK) LPAREN RPAREN OVER LPAREN (PARTITION BY groupByList)? (ORDER BY orderList)? RPAREN;

// Expressions
// Alternative order matters for ANTLR4's left-recursion precedence: earlier
// alternatives bind tighter. Comparisons/IN/EXISTS are the atoms; NOT binds
// tighter than AND, which binds tighter than OR - standard SQL precedence.
// Labels (#Name) give each alternative its own generated context class,
// making SqlParser.buildWhereExpr's tree-walk a straightforward instanceof
// switch instead of manually checking which optional token is non-null.
expression: LPAREN expression RPAREN                              #ParenExpr
          | NOT expression                                        #NotExpr
          | columnName ASSIGN columnName                           #EqColumnCompare
          | columnName GT columnName                                #GtColumnCompare
          | columnName LT columnName                                #LtColumnCompare
          | columnName GE columnName                                #GeColumnCompare
          | columnName LE columnName                                #LeColumnCompare
          | columnName NE columnName                                #NeColumnCompare
          | columnName ASSIGN literal                              #EqCompare
          | columnName GT literal                                  #GtCompare
          | columnName LT literal                                  #LtCompare
          | columnName GE literal                                  #GeCompare
          | columnName LE literal                                  #LeCompare
          | columnName NE literal                                  #NeCompare
          | columnName LIKE literal                                #LikeCompare
          | columnName CONTAINS literal                            #ContainsCompare
          | columnName ARRAY_CONTAINS literal                      #ArrayContainsCompare
          | columnName JSON_EXTRACT_TEXT literal ASSIGN literal    #JsonExtractTextEqCompare
          | LPAREN columnName COMMA columnName RPAREN OVERLAPS LPAREN literal COMMA literal RPAREN #RangeOverlapsCompare
          | columnName IN LPAREN valueList RPAREN                  #InListExpr
          | columnName NOT IN LPAREN valueList RPAREN               #NotInListExpr
          | columnName IN LPAREN select RPAREN                      #InSubqueryExpr
          | columnName NOT IN LPAREN select RPAREN                  #NotInSubqueryExpr
          | columnName ASSIGN LPAREN select RPAREN                  #EqSubqueryExpr
          | columnName GT LPAREN select RPAREN                      #GtSubqueryExpr
          | columnName LT LPAREN select RPAREN                      #LtSubqueryExpr
          | columnName GE LPAREN select RPAREN                      #GeSubqueryExpr
          | columnName LE LPAREN select RPAREN                      #LeSubqueryExpr
          | columnName NE LPAREN select RPAREN                      #NeSubqueryExpr
          | EXISTS LPAREN select RPAREN                              #ExistsExpr
          | NOT EXISTS LPAREN select RPAREN                          #NotExistsExpr
          | expression AND expression                                #AndExpr
          | expression OR expression                                 #OrExpr
          ;

// Values
valueList: insertValue (COMMA insertValue)*;
insertValue: literal | NEXTVAL LPAREN STRING_LITERAL RPAREN | CURRVAL LPAREN STRING_LITERAL RPAREN | arrayLiteral;
arrayLiteral: ARRAY LBRACKET (literal (COMMA literal)*)? RBRACKET;

// Data types
dataType: (INT | INTEGER | BIGINT | SMALLINT | TINYINT | SERIAL | BIGSERIAL
        | VARCHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | TEXT | CHAR (LPAREN INTEGER_LITERAL RPAREN)?
        | BOOLEAN | BOOL
        | DATE | TIME | TIMESTAMP
        | DECIMAL LPAREN INTEGER_LITERAL COMMA INTEGER_LITERAL RPAREN
        | DOUBLE | FLOAT
        | BYTEA | BLOB
        | UUID
        | JSON | JSONB) (LBRACKET RBRACKET)?;

// Literals
literal: STRING_LITERAL | INTEGER_LITERAL | FLOAT_LITERAL | BOOLEAN_LITERAL | NULL;
defaultValue: literal | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP | NEXTVAL LPAREN STRING_LITERAL RPAREN;

// Identifiers
tableName: IDENTIFIER;
columnName: IDENTIFIER (DOT IDENTIFIER)?;
alias: IDENTIFIER | STRING_LITERAL;
limitValue: INTEGER_LITERAL;

// Lexer rules
CREATE: C R E A T E;
TABLE: T A B L E;
VIEW: V I E W;
AS: A S;
WITH: W I T H;
UNION: U N I O N;
ALL: A L L;
RECURSIVE: R E C U R S I V E;
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
GROUP: G R O U P;
HAVING: H A V I N G;
LIMIT: L I M I T;
ASC: A S C;
DESC: D E S C;
AND: A N D;
OR: O R;
NOT: N O T;
IN: I N;
EXISTS: E X I S T S;
LIKE: L I K E;
IS: I S;
NULL: N U L L;
DEFAULT: D E F A U L T;
SHOW: S H O W;
TABLES: T A B L E S;
STATS: S T A T S;
SEQUENCE: S E Q U E N C E;
ROW_NUMBER: R O W '_' N U M B E R;
RANK: R A N K;
DENSE_RANK: D E N S E '_' R A N K;
OVER: O V E R;
PARTITION: P A R T I T I O N;
INCREMENT: I N C R E M E N T;
NEXTVAL: N E X T V A L;
CURRVAL: C U R R V A L;
SERIAL: S E R I A L;
BIGSERIAL: B I G S E R I A L;
INDEX: I N D E X;
ON: O N;
USING: U S I N G;
HASH: H A S H;
BTREE: B T R E E;
BRIN: B R I N;
GIN: G I N;
BITMAP: B I T M A P;
GIST: G I S T;
OVERLAPS: O V E R L A P S;
CONTAINS: C O N T A I N S;
JOIN: J O I N;
INNER: I N N E R;
EXPLAIN: E X P L A I N;
ANALYZE: A N A L Y Z E;
VACUUM: V A C U U M;
BEGIN: B E G I N;
START: S T A R T;
TRANSACTION: T R A N S A C T I O N;
COMMIT: C O M M I T;
ROLLBACK: R O L L B A C K;
SAVEPOINT: S A V E P O I N T;
RELEASE: R E L E A S E;
TO: T O;

COUNT: C O U N T;
SUM: S U M;
AVG: A V G;
MIN: M I N;
MAX: M A X;

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
ARRAY: A R R A Y;
LBRACKET: '[';
RBRACKET: ']';
JSON: J S O N;
JSONB: J S O N B;

CURRENT_DATE: C U R R E N T '_' D A T E;
CURRENT_TIME: C U R R E N T '_' T I M E;
CURRENT_TIMESTAMP: C U R R E N T '_' T I M E S T A M P;

// Operators and symbols
LPAREN: '(';
DOT: '.';
RPAREN: ')';
COMMA: ',';
SEMICOLON: ';';
ASSIGN: '=';
GT: '>';
LT: '<';
GE: '>=';
LE: '<=';
NE: '!=';
ARRAY_CONTAINS: '@>';
JSON_EXTRACT_TEXT: '->>';
STAR: '*';

// Literals
IDENTIFIER: [a-zA-Z_] [a-zA-Z0-9_]*;
STRING_LITERAL: '\'' (~['])* '\'';
INTEGER_LITERAL: [0-9]+;
FLOAT_LITERAL: [0-9]+ '.' [0-9]+;
BOOLEAN_LITERAL: TRUE | FALSE;

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